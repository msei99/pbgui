"""Lightweight PBCluster worker for local Cluster Sync reconciliation.

The worker owns local rebuild/materialization and will own peer fanout/pull in
the next phase. It intentionally does not start or stop bots; PBRun remains the
runtime gatekeeper and reads the local desired state.
"""

from __future__ import annotations

import json
import os
import base64
import hashlib
import shlex
import subprocess
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from cluster_sync_command import (
    ClusterSyncCommandError,
    MAX_GET_OPS,
    _append_credential_migration_acks,
    _materialize_api_keys,
    _materialize_credentials,
    _materialize_v7_configs,
    _validate_sealed_blob_payload,
)
from cmc_leases import ClusterMailbox
from credential_reconciler import reconcile_pending_credentials
from credential_migration import advance_local_credential_migration, run_credential_migration
from credential_rolling_bootstrap import bootstrap_local_legacy_credentials
from logging_helpers import human_log as _log
from master.cluster_state import (
    append_operation,
    ClusterPaths,
    ClusterStateError,
    build_config_manifest,
    compute_config_manifest_hash,
    default_cluster_root,
    load_operations,
    normalize_node_sync_mode,
    read_local_identity,
    rebuild_materialized_state,
    stage_membership_operations,
    validate_operation,
    V2_CREDENTIAL_OPS,
    write_operation,
)
from secure_files import atomic_write_private_bytes, ensure_private_directory_tree
from master.cluster_ssh_keys import ensure_cluster_ssh_key
from pbgui_purefunc import PBGDIR

SERVICE = "PBCluster"
STATUS_SCHEMA_VERSION = 1
DEFAULT_SSH_TIMEOUT = 30
CONFIG_BLOB_BATCH_TARGET_BYTES = 12 * 1024 * 1024
APPLY_BUNDLE_TARGET_BYTES = 12 * 1024 * 1024
DEFAULT_PEER_WORKERS = 32


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """Atomically write one JSON status file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4)
        f.write("\n")
    os.replace(tmp, path)


class ClusterSyncWorker:
    """Run local Cluster Sync maintenance for one PBGui installation."""

    def __init__(
        self,
        pbgdir: Path | str | None = None,
        *,
        interval: int = 60,
        boot_window: int = 20,
        peer_workers: int = DEFAULT_PEER_WORKERS,
        peer_client: Any | None = None,
    ) -> None:
        """Initialize a worker for *pbgdir*."""

        self.pbgdir = Path(pbgdir or PBGDIR)
        self.cluster_root = default_cluster_root(self.pbgdir)
        self.interval = max(5, int(interval))
        self.boot_window = max(0, int(boot_window))
        self.peer_workers = max(1, int(peer_workers))
        self.status_path = ClusterPaths.from_root(self.cluster_root).root / "sync_status.json"
        self.trigger_path = ClusterPaths.from_root(self.cluster_root).root / "sync_request"
        self.peer_client = peer_client or SshClusterPeerClient(cluster_root=self.cluster_root)
        self._peer_backoff: dict[str, dict[str, Any]] = {}
        self._local_state_lock = threading.Lock()
        self._last_trigger_mtime = self._trigger_mtime()
        self._stop = threading.Event()
        self._sync_requested = threading.Event()

    def request_sync(self) -> None:
        """Request a near-term sync pass."""

        self._sync_requested.set()

    def stop(self) -> None:
        """Stop the worker loop."""

        self._stop.set()
        self._sync_requested.set()

    def run_forever(self) -> None:
        """Run boot reconciliation and then periodic local maintenance."""

        _log(SERVICE, "PBCluster worker starting")
        self.run_once(reason="boot")
        next_periodic = time.time() + self.interval
        while not self._stop.is_set():
            wait_for = min(2.0, max(0.1, next_periodic - time.time()))
            triggered = self._sync_requested.wait(wait_for)
            if self._stop.is_set():
                break
            self._sync_requested.clear()
            trigger_changed = self._consume_trigger_change()
            periodic_due = time.time() >= next_periodic
            if not triggered and not trigger_changed and not periodic_due:
                continue
            reason = "event" if triggered or trigger_changed else "periodic"
            self.run_once(reason=reason)
            if periodic_due or reason == "periodic":
                next_periodic = time.time() + self.interval
        _log(SERVICE, "PBCluster worker stopped")

    def _trigger_mtime(self) -> float:
        """Return the current sync-request trigger mtime."""

        try:
            return self.trigger_path.stat().st_mtime
        except OSError:
            return 0.0

    def _consume_trigger_change(self) -> bool:
        """Return True once for each observed sync-request trigger update."""

        current = self._trigger_mtime()
        if current <= self._last_trigger_mtime:
            return False
        self._last_trigger_mtime = current
        return True

    def run_once(self, *, reason: str = "manual") -> dict[str, Any]:
        """Run one local sync pass and return the written status payload."""

        if str(reason or "") == "event":
            self._peer_backoff.clear()
        started_at = int(time.time())
        base: dict[str, Any] = {
            "schema_version": STATUS_SCHEMA_VERSION,
            "service": SERVICE,
            "reason": str(reason or "manual"),
            "started_at": started_at,
            "finished_at": started_at,
            "ok": False,
            "status": "unknown",
            "cluster_root": str(self.cluster_root),
            "network_sync": "not_implemented",
            "boot_window_seconds": self.boot_window,
        }

        try:
            base["credential_bootstrap"] = bootstrap_local_legacy_credentials(self.pbgdir)
        except Exception as exc:
            base["credential_bootstrap"] = {"status": "error", "error": type(exc).__name__}
            _log(SERVICE, f"Local credential bootstrap pending: {type(exc).__name__}", level="WARNING")

        try:
            identity = read_local_identity(self.cluster_root)
            try:
                base["cluster_ssh"] = _compact_cluster_ssh_key(ensure_cluster_ssh_key(self.cluster_root, node_id=str(identity.get("node_id") or "")))
            except Exception as exc:
                base["cluster_ssh"] = {"ok": False, "error": str(exc)}
                _log(SERVICE, f"Cluster SSH key setup failed: {exc}", level="WARNING")
        except ClusterStateError as exc:
            status = dict(base)
            status.update({
                "status": "not_configured",
                "error": str(exc),
                "finished_at": int(time.time()),
            })
            _atomic_write_json(self.status_path, status)
            return status

        try:
            try:
                migration_pre_sync = advance_local_credential_migration(
                    self.pbgdir,
                    max_items=8,
                )
            except Exception as exc:
                migration_pre_sync = {"status": "error", "error": type(exc).__name__}
                _log(SERVICE, f"Credential migration advance pending: {type(exc).__name__}", level="WARNING")
            materialized = rebuild_materialized_state(self.cluster_root)
            peer_results = self._sync_peers(identity, materialized)
            if any(int(item.get("pulled_ops") or 0) for item in peer_results):
                materialized = rebuild_materialized_state(self.cluster_root)
            migration_coordinator = {"status": "not_coordinator"}
            if str(identity.get("role") or "").strip().lower() == "master":
                try:
                    coordinator_state = run_credential_migration(self.pbgdir)
                    migration_coordinator = {
                        "status": str(coordinator_state.get("status") or "advanced"),
                        "phase": str(coordinator_state.get("phase") or "unknown"),
                    }
                    materialized = rebuild_materialized_state(self.cluster_root)
                except Exception as exc:
                    migration_coordinator = {"status": "error", "error": type(exc).__name__}
                    _log(SERVICE, f"Credential migration coordinator pending: {type(exc).__name__}", level="WARNING")
            v7_result = _materialize_v7_configs(self.cluster_root, write=True)
            api_preview = _materialize_api_keys(self.cluster_root, write=False)
            api_result = api_preview
            if api_preview.get("can_apply"):
                api_result = _materialize_api_keys(self.cluster_root, write=True)
            credential_preview = _materialize_credentials(self.cluster_root, write=False)
            credential_result = credential_preview
            if credential_preview.get("can_apply"):
                credential_result = _materialize_credentials(self.cluster_root, write=True)
            migration_ack = _append_credential_migration_acks(self.cluster_root)
            try:
                migration_post_sync = advance_local_credential_migration(
                    self.pbgdir,
                    max_items=8,
                )
            except Exception as exc:
                migration_post_sync = {"status": "error", "error": type(exc).__name__}
                _log(SERVICE, f"Credential migration post-sync advance pending: {type(exc).__name__}", level="WARNING")
            try:
                credential_reconciliation = reconcile_pending_credentials(self.pbgdir)
            except Exception as exc:
                credential_reconciliation = {
                    "status": "error",
                    "error": type(exc).__name__,
                }
                _log(SERVICE, f"Credential reconciliation pending: {type(exc).__name__}", level="WARNING")

            cluster_nodes = materialized.get("cluster_nodes") if isinstance(materialized, dict) else {}
            nodes = cluster_nodes.get("nodes") if isinstance(cluster_nodes, dict) else {}
            nodes = nodes if isinstance(nodes, dict) else {}
            local_node_id = str(identity.get("node_id") or "")
            peer_count = sum(
                1
                for node_id, node in nodes.items()
                if str(node_id) != local_node_id and isinstance(node, dict) and node.get("enabled", True) is not False
            )
            migration_advance_status = {
                "pre_sync": migration_pre_sync,
                "post_sync": migration_post_sync,
            }
            if migration_coordinator.get("status") != "not_coordinator":
                migration_advance_status["coordinator"] = migration_coordinator
            status = dict(base)
            status.update({
                "ok": True,
                "status": "local_reconciled",
                "cluster_id": str(identity.get("cluster_id") or ""),
                "node_id": local_node_id,
                "finished_at": int(time.time()),
                "generation": int(cluster_nodes.get("generation") or 0) if isinstance(cluster_nodes, dict) else 0,
                "state_vector": materialized.get("state_vector") or {},
                "peers_total": peer_count,
                "peers": peer_results,
                "peers_ok": sum(1 for item in peer_results if item.get("ok")),
                "pulled_ops": sum(int(item.get("pulled_ops") or 0) for item in peer_results),
                "pushed_ops": sum(int(item.get("pushed_ops") or 0) for item in peer_results),
                "mailbox_pulled": sum(int(item.get("mailbox_pulled") or 0) for item in peer_results),
                "mailbox_pushed": sum(int(item.get("mailbox_pushed") or 0) for item in peer_results),
                "mailbox_acked": sum(int(item.get("mailbox_acked") or 0) for item in peer_results),
                "v7_materialization": _compact_materialization(v7_result),
                "api_key_materialization": _compact_materialization(api_result),
                "credential_materialization": _compact_materialization(credential_result),
                "credential_migration_ack": migration_ack,
                "credential_migration_advance": migration_advance_status,
                "credential_reconciliation": credential_reconciliation,
            })
            _atomic_write_json(self.status_path, status)
            return status
        except (ClusterStateError, ClusterSyncCommandError, ClusterSyncWorkerError, OSError, ValueError) as exc:
            _log(SERVICE, f"Cluster sync pass failed: {exc}", level="ERROR")
            status = dict(base)
            status.update({
                "status": "error",
                "error": str(exc),
                "finished_at": int(time.time()),
                "cluster_id": str(identity.get("cluster_id") or ""),
                "node_id": str(identity.get("node_id") or ""),
            })
            _atomic_write_json(self.status_path, status)
            return status

    def _sync_peers(self, identity: dict[str, Any], materialized: dict[str, Any]) -> list[dict[str, Any]]:
        """Synchronize with all currently known reachable peers."""

        cluster_nodes = materialized.get("cluster_nodes") if isinstance(materialized, dict) else {}
        nodes = cluster_nodes.get("nodes") if isinstance(cluster_nodes, dict) else {}
        nodes = nodes if isinstance(nodes, dict) else {}
        local_node_id = str(identity.get("node_id") or "")
        results_by_peer: dict[str, dict[str, Any]] = {}
        pending: list[tuple[str, dict[str, Any]]] = []
        for peer_id in sorted(nodes):
            peer = nodes.get(peer_id) if isinstance(nodes.get(peer_id), dict) else {}
            if str(peer_id) == local_node_id:
                continue
            local_node = nodes.get(local_node_id) if isinstance(nodes.get(local_node_id), dict) else {}
            peer_mode = _peer_sync_mode(peer)
            if peer_mode == "disabled":
                results_by_peer[str(peer_id)] = _peer_result(peer_id, peer, ok=False, status="disabled", reason="sync is disabled")
                continue
            if peer_mode == "outbound_only":
                results_by_peer[str(peer_id)] = _peer_result(peer_id, peer, ok=True, status="outbound_only", reason="peer is outbound-only")
                continue
            allowed, reason = _peer_topology_allows(local_node, str(peer_id), peer)
            if not allowed:
                results_by_peer[str(peer_id)] = _peer_result(peer_id, peer, ok=True, status="topology_skipped", reason=reason)
                continue
            if not str(peer.get("ssh_host") or "").strip():
                results_by_peer[str(peer_id)] = _peer_result(peer_id, peer, ok=False, status="config_error", reason="reachable peer has no ssh_host")
                continue
            backoff = self._peer_backoff.get(str(peer_id)) or {}
            next_retry = float(backoff.get("next_retry") or 0)
            if next_retry and time.time() < next_retry:
                result = _peer_result(peer_id, peer, ok=False, status="backoff", reason=str(backoff.get("error") or "previous sync failed"))
                result["next_retry"] = int(next_retry)
                results_by_peer[str(peer_id)] = result
                continue
            pending.append((str(peer_id), peer))

        if pending:
            max_workers = min(self.peer_workers, len(pending))
            with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="pbcluster-peer") as executor:
                future_map = {
                    executor.submit(self._sync_peer_with_backoff, peer_id, peer, local_node_id, str(identity.get("cluster_id") or "")): peer_id
                    for peer_id, peer in pending
                }
                for future in as_completed(future_map):
                    peer_id = future_map[future]
                    results_by_peer[peer_id] = future.result()

        return [results_by_peer[str(peer_id)] for peer_id in sorted(nodes) if str(peer_id) != local_node_id and str(peer_id) in results_by_peer]

    def _sync_peer_with_backoff(self, peer_id: str, peer: dict[str, Any], local_node_id: str, cluster_id: str) -> dict[str, Any]:
        """Synchronize one peer and update its retry backoff."""

        backoff = self._peer_backoff.get(str(peer_id)) or {}
        try:
            result = self._sync_peer(peer, local_node_id, cluster_id)
            self._peer_backoff.pop(str(peer_id), None)
            return result
        except Exception as exc:
            failures = int(backoff.get("failures") or 0) + 1
            delay = min(600, max(30, 30 * (2 ** min(failures - 1, 4))))
            self._peer_backoff[str(peer_id)] = {
                "failures": failures,
                "next_retry": time.time() + delay,
                "error": str(exc),
            }
            _log(SERVICE, f"Peer sync failed for {peer.get('pbname') or peer_id}: {exc}", level="WARNING")
            result = _peer_result(str(peer_id), peer, ok=False, status="error", reason=str(exc))
            result["retry_delay"] = delay
            return result

    def _sync_peer(self, peer: dict[str, Any], local_node_id: str, cluster_id: str) -> dict[str, Any]:
        """Synchronize local state with one peer."""

        peer_id = str(peer.get("node_id") or "")
        base_result = _peer_result(peer_id, peer, ok=True, status="synced")
        hello, remote_vector = self._peer_handshake(peer, local_node_id)
        if str(hello.get("cluster_id") or "") != cluster_id:
            raise ClusterSyncWorkerError("peer belongs to another cluster")
        local_materialized = rebuild_materialized_state(self.cluster_root, write=False)
        cutoff = (((local_materialized.get("desired_state") or {}).get("credential_migration") or {}).get("cutoff"))
        if isinstance(cutoff, dict) and int(hello.get("protocol_version") or 0) < int(cutoff.get("min_protocol") or 2):
            raise ClusterSyncWorkerError("credential protocol downgrade rejected after cutoff")
        remote_node_id = str(hello.get("node_id") or "")
        if peer_id and remote_node_id and remote_node_id != peer_id:
            raise ClusterSyncWorkerError("peer node_id does not match cluster_nodes")
        with self._local_state_lock:
            key_changed = self._record_peer_cluster_ssh_metadata(peer, hello)

        local_ops = load_operations(self.cluster_root, expected_cluster_id=cluster_id)
        local_vector = _state_vector_from_operations(local_ops)

        pulled_ops = self._pull_missing_operations(peer, local_node_id, remote_vector, local_vector, cluster_id)
        if pulled_ops:
            with self._local_state_lock:
                rebuild_materialized_state(self.cluster_root)
                local_ops = load_operations(self.cluster_root, expected_cluster_id=cluster_id)
            local_vector = _state_vector_from_operations(local_ops)

        push_ops = _select_operations_missing_on_remote(local_ops, remote_vector)
        peer_supports_credentials = (
            int(hello.get("protocol_version") or 1) >= 2
            and bool((hello.get("credential_capability") or {}).get("sealed_credentials"))
        )
        deferred_credential_ops = 0
        if not peer_supports_credentials:
            deferred_credential_ops = sum(
                1 for operation in push_ops if str(operation.get("op") or "") in V2_CREDENTIAL_OPS
            )
            push_ops = [
                operation
                for operation in push_ops
                if str(operation.get("op") or "") not in V2_CREDENTIAL_OPS
            ]
        pushed_config_blobs = 0
        pushed_secret_blobs = 0
        pushed_sealed_blobs = 0
        pushed_ops = 0
        if push_ops:
            fast_result = self._apply_operations_bundle(peer, local_node_id, push_ops)
            if fast_result is None:
                pushed_config_blobs, pushed_secret_blobs, pushed_sealed_blobs = self._push_blobs_for_operations(peer, local_node_id, push_ops)
                pushed_ops = self._push_operations(peer, local_node_id, push_ops)
                if pushed_ops:
                    self._remote_rebuild_and_materialize(peer, local_node_id)
            else:
                pushed_config_blobs = int(fast_result.get("config_blobs") or 0)
                pushed_secret_blobs = int(fast_result.get("secret_blobs") or 0)
                pushed_sealed_blobs = int(fast_result.get("sealed_blobs") or 0)
                pushed_ops = int(fast_result.get("count") or len(push_ops))
                base_result["remote_apply"] = "bundle"

        mailbox_counts = (
            self._sync_mailbox(peer, local_node_id)
            if bool(hello.get("mailbox_capability"))
            else {"supported": False, "pulled": 0, "pushed": 0, "acked": 0}
        )

        if pulled_ops or pushed_ops or key_changed or mailbox_counts["pulled"] or mailbox_counts["pushed"]:
            base_result["status"] = "changed"
        base_result.update({
            "remote_node_id": remote_node_id,
            "remote_vector": remote_vector,
            "local_vector": local_vector,
            "pulled_ops": pulled_ops,
            "pushed_ops": pushed_ops,
            "pushed_config_blobs": pushed_config_blobs,
            "pushed_secret_blobs": pushed_secret_blobs,
            "pushed_sealed_blobs": pushed_sealed_blobs,
            "deferred_credential_ops": deferred_credential_ops,
            "cluster_ssh_key_updated": key_changed,
            "mailbox_supported": mailbox_counts["supported"],
            "mailbox_pulled": mailbox_counts["pulled"],
            "mailbox_pushed": mailbox_counts["pushed"],
            "mailbox_acked": mailbox_counts["acked"],
            "last_seen": int(time.time()),
        })
        return base_result

    def _sync_mailbox(self, peer: dict[str, Any], local_node_id: str) -> dict[str, Any]:
        """Exchange missing mailbox messages while retaining opaque relay payloads."""

        counts: dict[str, Any] = {"supported": False, "pulled": 0, "pushed": 0, "acked": 0}
        try:
            remote_payload = self.peer_client.run(peer, local_node_id, "get-mailbox-index")
        except Exception as exc:
            if _is_unsupported_command_error(exc, "get-mailbox-index"):
                return counts
            raise
        counts["supported"] = True
        remote_items = remote_payload.get("messages") if isinstance(remote_payload, dict) else []
        remote_items = remote_items if isinstance(remote_items, list) else []
        remote_ids = {
            str(item.get("message_id") or "")
            for item in remote_items
            if isinstance(item, dict) and item.get("message_id")
        }
        mailbox = ClusterMailbox(self.cluster_root)
        local_ids = {str(item["message_id"]) for item in mailbox.index()}

        for message_id in sorted(remote_ids - local_ids):
            payload = self.peer_client.run(
                peer,
                local_node_id,
                f"get-mailbox-message {shlex.quote(message_id)}",
            )
            message = payload.get("message") if isinstance(payload, dict) else None
            if not isinstance(message, dict):
                raise ClusterSyncWorkerError("peer returned invalid mailbox message")
            if mailbox.put(message):
                counts["pulled"] += 1
            self.peer_client.run(
                peer,
                local_node_id,
                f"ack-mailbox-message {shlex.quote(message_id)}",
            )
            counts["acked"] += 1

        for item in mailbox.index():
            message_id = str(item["message_id"])
            if message_id in remote_ids:
                continue
            message = mailbox.get(message_id)
            result = self.peer_client.run(
                peer,
                local_node_id,
                "put-mailbox-message",
                payload=json.dumps(message, sort_keys=True, separators=(",", ":")),
            )
            if bool(result.get("created", True)):
                counts["pushed"] += 1
            mailbox.ack(message_id, str(peer.get("node_id") or ""))
            counts["acked"] += 1
        return counts

    def _peer_handshake(self, peer: dict[str, Any], local_node_id: str) -> tuple[dict[str, Any], dict[str, int]]:
        """Return peer hello metadata and state vector, using one SSH call when supported."""

        try:
            payload = self.peer_client.run(peer, local_node_id, "handshake")
            return payload, _as_state_vector(payload.get("state_vector") or {})
        except Exception as exc:
            if not _is_unsupported_command_error(exc, "handshake"):
                raise
        hello = self.peer_client.run(peer, local_node_id, "hello")
        remote_vector_payload = self.peer_client.run(peer, local_node_id, "get-state-vector")
        return hello, _as_state_vector(remote_vector_payload.get("state_vector") or {})

    def _record_peer_cluster_ssh_metadata(self, peer: dict[str, Any], hello: dict[str, Any]) -> bool:
        """Record peer Cluster SSH public key metadata when hello exposes it."""

        peer_id = str(peer.get("node_id") or hello.get("node_id") or "")
        public_key = str(hello.get("cluster_ssh_public_key") or "").strip()
        fingerprint = str(hello.get("cluster_ssh_fingerprint") or "").strip()
        updates: dict[str, Any] = {}
        if public_key and fingerprint:
            updates.update({
                "cluster_ssh_public_key": public_key,
                "cluster_ssh_fingerprint": fingerprint,
                "cluster_ssh_mode": "forced",
            })
        if not peer_id or not updates:
            return False
        if all(peer.get(field) == value for field, value in updates.items()):
            return False
        append_operation(
            self.cluster_root,
            "UPDATE_NODE",
            {"node_id": peer_id, **updates},
        )
        return True

    def _pull_missing_operations(
        self,
        peer: dict[str, Any],
        local_node_id: str,
        remote_vector: dict[str, int],
        local_vector: dict[str, int],
        cluster_id: str,
    ) -> int:
        """Pull operation ranges that exist on the peer but not locally."""

        pulled = 0
        deferred_v2: list[dict[str, Any]] = []
        for actor in sorted(remote_vector):
            remote_seq = int(remote_vector.get(actor) or 0)
            local_seq = int(local_vector.get(actor) or 0)
            if remote_seq <= local_seq:
                continue
            start = local_seq + 1
            while start <= remote_seq:
                end = min(remote_seq, start + MAX_GET_OPS - 1)
                payload = self.peer_client.run(peer, local_node_id, f"get-ops {shlex.quote(actor)} {start} {end}")
                operations = payload.get("operations") if isinstance(payload, dict) else []
                operations = operations if isinstance(operations, list) else []
                staged_trust = stage_membership_operations(
                    self.cluster_root,
                    operations,
                    expected_cluster_id=cluster_id,
                    authenticated_remote_node=str(peer.get("node_id") or ""),
                )
                self._pull_blobs_for_operations(peer, local_node_id, operations)
                for operation in operations:
                    if str(operation.get("op") or "") in V2_CREDENTIAL_OPS:
                        deferred_v2.append(operation)
                        continue
                    validate_operation(
                        operation,
                        expected_cluster_id=cluster_id,
                        cluster_root=self.cluster_root,
                        membership_trust=staged_trust,
                        network_input=True,
                    )
                    op_path = ClusterPaths.from_root(self.cluster_root).oplog / str(operation["actor"]) / f"{int(operation['seq']):08d}.json"
                    with self._local_state_lock:
                        existed = op_path.exists()
                        write_operation(
                            self.cluster_root,
                            operation,
                            network_input=True,
                            membership_trust=staged_trust,
                        )
                    if not existed:
                        pulled += 1
                start = end + 1
        for operation in deferred_v2:
            self._pull_blobs_for_operations(peer, local_node_id, [operation])
            validate_operation(
                operation,
                expected_cluster_id=cluster_id,
                cluster_root=self.cluster_root,
                network_input=True,
            )
            op_path = (
                ClusterPaths.from_root(self.cluster_root).oplog
                / str(operation["actor"])
                / f"{int(operation['seq']):08d}.json"
            )
            with self._local_state_lock:
                existed = op_path.exists()
                write_operation(self.cluster_root, operation, network_input=True)
            if not existed:
                pulled += 1
        return pulled

    def _pull_blobs_for_operations(self, peer: dict[str, Any], local_node_id: str, operations: list[dict[str, Any]]) -> dict[str, int]:
        """Pull required config and secret blobs for received operations."""

        counts = {"config": 0, "secret": 0, "sealed": 0}
        paths = ClusterPaths.from_root(self.cluster_root)
        materialized = rebuild_materialized_state(self.cluster_root, write=False)
        cutoff = (((materialized.get("desired_state") or {}).get("credential_migration") or {}).get("cutoff"))
        obsolete_secret_hashes = set((cutoff or {}).get("obsolete_secret_blob_hashes") or [])
        obsolete_secret_hashes.update(
            str(blob_hash)
            for operation in operations
            if str(operation.get("op") or "") == "CREDENTIAL_CUTOFF"
            for blob_hash in operation.get("obsolete_secret_blob_hashes") or []
        )
        for operation in operations:
            refs = _operation_hash_refs(operation)
            for manifest_hash in refs["config"]:
                raw = self._ensure_remote_blob(peer, local_node_id, paths.config_blobs, manifest_hash, secret=False)
                counts["config"] += 1 if raw is not None else 0
                if raw:
                    for blob_hash in _manifest_file_hashes(raw):
                        if self._ensure_remote_blob(peer, local_node_id, paths.config_blobs, blob_hash, secret=False) is not None:
                            counts["config"] += 1
            for payload_hash in refs["api_payload"]:
                if self._ensure_remote_blob(peer, local_node_id, paths.config_blobs, payload_hash, secret=False) is not None:
                    counts["config"] += 1
            for secret_hash in refs["secret"]:
                if secret_hash in obsolete_secret_hashes:
                    continue
                if self._ensure_remote_blob(peer, local_node_id, paths.secret_blobs, secret_hash, secret=True) is not None:
                    counts["secret"] += 1
            for sealed_hash in refs["sealed"]:
                if self._ensure_remote_blob(
                    peer,
                    local_node_id,
                    paths.sealed_blobs,
                    sealed_hash,
                    secret=True,
                    sealed=True,
                ) is not None:
                    counts["sealed"] += 1
        return counts

    def _ensure_remote_blob(
        self,
        peer: dict[str, Any],
        local_node_id: str,
        base_dir: Path,
        blob_hash: str,
        *,
        secret: bool,
        sealed: bool = False,
    ) -> bytes | None:
        """Ensure one local blob exists by pulling it from the peer when missing."""

        if _local_blob_exists(base_dir, blob_hash):
            return None
        verb = "get-sealed-blob" if sealed else "get-secret-blob" if secret else "get-blob"
        payload = self.peer_client.run(peer, local_node_id, f"{verb} {shlex.quote(str(blob_hash))}")
        raw = base64.b64decode(str(payload.get("content_b64") or ""))
        if sealed:
            _validate_sealed_blob_payload(self.cluster_root, raw)
        _write_local_blob(base_dir, str(payload.get("hash") or blob_hash), raw, secret=secret)
        return raw

    def _push_blobs_for_operations(self, peer: dict[str, Any], local_node_id: str, operations: list[dict[str, Any]]) -> tuple[int, int, int]:
        """Push required config and secret blobs for outbound operations."""

        config_blobs, secret_blobs, sealed_blobs = _collect_local_blobs_for_operations(self.cluster_root, operations)
        pushed_config = 0
        for chunk in _chunk_config_blobs(config_blobs):
            payload = _blob_batch_payload(chunk)
            result = self.peer_client.run(peer, local_node_id, "put-blobs", payload=payload)
            pushed_config += int(result.get("count") or len(chunk))
        pushed_secret = 0
        for blob in secret_blobs:
            self.peer_client.run(peer, local_node_id, f"put-secret-blob {shlex.quote(str(blob['hash']))}", payload=blob["raw"])
            pushed_secret += 1
        pushed_sealed = 0
        for blob in sealed_blobs:
            self.peer_client.run(peer, local_node_id, f"put-sealed-blob {shlex.quote(str(blob['hash']))}", payload=blob["raw"])
            pushed_sealed += 1
        return pushed_config, pushed_secret, pushed_sealed

    def _push_operations(self, peer: dict[str, Any], local_node_id: str, operations: list[dict[str, Any]]) -> int:
        """Push outbound operations to the peer."""

        if not operations:
            return 0
        payload = json.dumps({"operations": operations}, sort_keys=True, separators=(",", ":"))
        result = self.peer_client.run(peer, local_node_id, "put-ops", payload=payload)
        return int(result.get("count") or len(operations))

    def _apply_operations_bundle(self, peer: dict[str, Any], local_node_id: str, operations: list[dict[str, Any]]) -> dict[str, Any] | None:
        """Push blobs and operations, then materialize the peer in one remote command."""

        if not operations:
            return {"ok": True, "count": 0, "config_blobs": 0, "secret_blobs": 0}
        config_blobs, secret_blobs, sealed_blobs = _collect_local_blobs_for_operations(self.cluster_root, operations)
        payload = _apply_bundle_payload(operations, config_blobs, secret_blobs, sealed_blobs)
        if len(payload.encode("utf-8")) > APPLY_BUNDLE_TARGET_BYTES:
            return None
        try:
            return self.peer_client.run(peer, local_node_id, "apply-bundle", payload=payload)
        except Exception as exc:
            if _is_unsupported_command_error(exc, "apply-bundle"):
                return None
            raise

    def _remote_rebuild_and_materialize(self, peer: dict[str, Any], local_node_id: str) -> None:
        """Rebuild and materialize local files on a peer after a successful push."""

        self.peer_client.run(peer, local_node_id, "rebuild")
        self.peer_client.run(peer, local_node_id, "materialize-v7")
        api_preview = self.peer_client.run(peer, local_node_id, "materialize-api-keys-preview")
        if api_preview.get("can_apply"):
            self.peer_client.run(peer, local_node_id, "materialize-api-keys")
        credential_preview = self.peer_client.run(peer, local_node_id, "materialize-credentials-preview")
        if credential_preview.get("can_apply"):
            self.peer_client.run(peer, local_node_id, "materialize-credentials")


class ClusterSyncWorkerError(RuntimeError):
    """Raised when PBCluster peer synchronization fails."""


class SshClusterPeerClient:
    """Small SSH client for restricted Cluster Sync peer commands."""

    def __init__(self, *, timeout: int = DEFAULT_SSH_TIMEOUT, connect_timeout: int = 8, cluster_root: Path | str | None = None) -> None:
        """Initialize subprocess SSH timeouts."""

        self.timeout = int(timeout)
        self.connect_timeout = int(connect_timeout)
        self.cluster_root = Path(cluster_root) if cluster_root else default_cluster_root(Path(PBGDIR))

    def run(self, peer: dict[str, Any], local_node_id: str, command_text: str, payload: str | bytes | None = None) -> dict[str, Any]:
        """Run one Cluster Sync command on *peer* and parse its JSON response."""

        command = self._ssh_command(peer, local_node_id, command_text)
        input_data = payload
        text_mode = not isinstance(input_data, bytes)
        completed = subprocess.run(
            command,
            input=input_data,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=text_mode,
            timeout=self.timeout,
        )
        stdout = completed.stdout.decode("utf-8", errors="replace") if isinstance(completed.stdout, bytes) else str(completed.stdout or "")
        stderr = completed.stderr.decode("utf-8", errors="replace") if isinstance(completed.stderr, bytes) else str(completed.stderr or "")
        if completed.returncode != 0:
            raise ClusterSyncWorkerError(stderr.strip() or stdout.strip() or f"ssh exited with {completed.returncode}")
        try:
            return json.loads(stdout.strip().splitlines()[-1])
        except (IndexError, json.JSONDecodeError) as exc:
            raise ClusterSyncWorkerError("peer returned invalid JSON") from exc

    def _ssh_command(self, peer: dict[str, Any], local_node_id: str, command_text: str) -> list[str]:
        """Build an ssh command for one peer."""

        host = str(peer.get("ssh_host") or "").strip()
        if not host:
            raise ClusterSyncWorkerError("reachable peer has no ssh_host")
        user = str(peer.get("ssh_user") or "").strip()
        target = f"{user}@{host}" if user else host
        try:
            port = int(peer.get("ssh_port") or 22)
        except (TypeError, ValueError):
            port = 22
        key = ensure_cluster_ssh_key(self.cluster_root)
        private_key = str(key.get("private_key_path") or "")
        command = str(command_text or "")
        if str(peer.get("cluster_ssh_mode") or "forced").strip().lower() == "direct":
            command = _remote_cluster_command(str(peer.get("remote_pbgui_dir") or "software/pbgui"), local_node_id, command)
        return [
            "ssh",
            "-i", private_key,
            "-o", "IdentitiesOnly=yes",
            "-o", "BatchMode=yes",
            "-o", "StrictHostKeyChecking=yes",
            "-o", f"ConnectTimeout={self.connect_timeout}",
            "-p", str(port),
            target,
            command,
        ]


def _compact_materialization(payload: dict[str, Any]) -> dict[str, Any]:
    """Return small status details for one materialization result."""

    counts = payload.get("counts") if isinstance(payload, dict) else {}
    return {
        "ok": bool(payload.get("ok", False)) if isinstance(payload, dict) else False,
        "action": str(payload.get("action") or "") if isinstance(payload, dict) else "",
        "status": str(payload.get("status") or "") if isinstance(payload, dict) else "",
        "reason": str(payload.get("reason") or "") if isinstance(payload, dict) else "",
        "counts": counts if isinstance(counts, dict) else {},
    }


def _compact_cluster_ssh_key(payload: dict[str, Any]) -> dict[str, Any]:
    """Return non-secret Cluster SSH key status details."""

    return {
        "ok": True,
        "created": bool(payload.get("created")),
        "public_key_path": str(payload.get("public_key_path") or ""),
        "fingerprint": str(payload.get("fingerprint") or ""),
    }


def _peer_sync_mode(peer: dict[str, Any]) -> str:
    """Return the effective peer sync mode for a materialized node."""

    if not isinstance(peer, dict):
        return "disabled"
    if peer.get("enabled", True) is False:
        return "disabled"
    if peer.get("state_replica", True) is False:
        return "disabled"
    return normalize_node_sync_mode(peer)


def _peer_topology_allows(local_node: dict[str, Any], peer_id: str, peer: dict[str, Any]) -> tuple[bool, str]:
    """Return whether this node should actively contact *peer* by default."""

    explicit = local_node.get("sync_peers") if isinstance(local_node, dict) else None
    if isinstance(explicit, list):
        if str(peer_id) in {str(item) for item in explicit}:
            return True, "explicit sync peer"
        return False, "peer is not in sync_peers"
    if str((local_node or {}).get("role") or "").strip() == "vps":
        return False, "VPS nodes do not initiate peer SSH without explicit sync_peers"
    return True, ""


def _peer_result(peer_id: str, peer: dict[str, Any], *, ok: bool, status: str, reason: str = "") -> dict[str, Any]:
    """Build a compact per-peer status row."""

    return {
        "node_id": str(peer_id or peer.get("node_id") or ""),
        "pbname": str(peer.get("pbname") or peer.get("hostname") or ""),
        "ok": bool(ok),
        "status": str(status or "unknown"),
        "reason": str(reason or ""),
        "pulled_ops": 0,
        "pushed_ops": 0,
        "pushed_config_blobs": 0,
        "pushed_secret_blobs": 0,
        "pushed_sealed_blobs": 0,
        "deferred_credential_ops": 0,
        "mailbox_supported": False,
        "mailbox_pulled": 0,
        "mailbox_pushed": 0,
        "mailbox_acked": 0,
    }


def _as_state_vector(value: Any) -> dict[str, int]:
    """Normalize a state-vector mapping."""

    result: dict[str, int] = {}
    if not isinstance(value, dict):
        return result
    for actor, seq in value.items():
        try:
            parsed = int(seq)
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            result[str(actor)] = parsed
    return result


def _state_vector_from_operations(operations: list[dict[str, Any]]) -> dict[str, int]:
    """Build a state vector from loaded operations."""

    sequences: dict[str, set[int]] = {}
    for operation in operations:
        actor = str(operation.get("actor") or "")
        try:
            seq = int(operation.get("seq") or 0)
        except (TypeError, ValueError):
            continue
        if actor and seq > 0:
            sequences.setdefault(actor, set()).add(seq)
    vector: dict[str, int] = {}
    for actor, values in sequences.items():
        contiguous = 0
        while contiguous + 1 in values:
            contiguous += 1
        if contiguous:
            vector[actor] = contiguous
    return {key: vector[key] for key in sorted(vector)}


def _select_operations_missing_on_remote(local_operations: list[dict[str, Any]], remote_vector: dict[str, int]) -> list[dict[str, Any]]:
    """Return local operations whose actor sequence is above the remote vector."""

    selected: list[dict[str, Any]] = []
    for operation in local_operations:
        actor = str(operation.get("actor") or "")
        try:
            seq = int(operation.get("seq") or 0)
        except (TypeError, ValueError):
            continue
        if actor and seq > int(remote_vector.get(actor, 0)):
            selected.append(dict(operation))
    selected.sort(key=lambda item: (str(item.get("actor") or ""), int(item.get("seq") or 0), str(item.get("op_id") or "")))
    return selected


def _operation_hash_refs(operation: dict[str, Any]) -> dict[str, list[str]]:
    """Return blob hashes referenced by one operation."""

    refs = {"config": [], "api_payload": [], "secret": [], "sealed": []}
    config_hash = str(operation.get("config_manifest_hash") or "")
    if config_hash:
        refs["config"].append(config_hash)
    payload_hash = str(operation.get("payload_hash") or "")
    if payload_hash:
        refs["api_payload"].append(payload_hash)
    secret_hash = str(operation.get("secret_blob_hash") or "")
    if secret_hash:
        refs["secret"].append(secret_hash)
    sealed_hash = str(operation.get("sealed_blob_hash") or "")
    if sealed_hash:
        refs["sealed"].append(sealed_hash)
    return refs


def _chunk_config_blobs(blobs: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Split config blobs into payload-size chunks."""

    chunks: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_size = len('{"blobs":[]}')
    for blob in blobs:
        raw = blob.get("raw") or b""
        encoded_size = len(str(blob.get("hash") or "")) + len(base64.b64encode(raw)) + 64
        if current and current_size + encoded_size > CONFIG_BLOB_BATCH_TARGET_BYTES:
            chunks.append(current)
            current = []
            current_size = len('{"blobs":[]}')
        current.append(blob)
        current_size += encoded_size
    if current:
        chunks.append(current)
    return chunks


def _blob_batch_payload(blobs: list[dict[str, Any]]) -> str:
    """Build a JSON base64 payload for put-blobs."""

    return json.dumps({
        "blobs": [
            {"hash": str(blob.get("hash") or ""), "content_b64": base64.b64encode(blob.get("raw") or b"").decode("ascii")}
            for blob in blobs
        ]
    }, sort_keys=True, separators=(",", ":"))


def _apply_bundle_payload(
    operations: list[dict[str, Any]],
    config_blobs: list[dict[str, Any]],
    secret_blobs: list[dict[str, Any]],
    sealed_blobs: list[dict[str, Any]] | None = None,
) -> str:
    """Build one JSON payload for apply-bundle."""

    def blob_items(blobs: list[dict[str, Any]]) -> list[dict[str, str]]:
        return [
            {"hash": str(blob.get("hash") or ""), "content_b64": base64.b64encode(blob.get("raw") or b"").decode("ascii")}
            for blob in blobs
        ]

    return json.dumps(
        {
            "operations": operations,
            "config_blobs": blob_items(config_blobs),
            "secret_blobs": blob_items(secret_blobs),
            "sealed_blobs": blob_items(sealed_blobs or []),
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _collect_local_blobs_for_operations(
    cluster_root: Path,
    operations: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Collect local config and secret blobs required to send operations."""

    paths = ClusterPaths.from_root(cluster_root)
    config_by_hash: dict[str, dict[str, Any]] = {}
    secret_by_hash: dict[str, dict[str, Any]] = {}
    sealed_by_hash: dict[str, dict[str, Any]] = {}
    materialized = rebuild_materialized_state(cluster_root, write=False)
    cutoff = (((materialized.get("desired_state") or {}).get("credential_migration") or {}).get("cutoff"))
    obsolete_secret_hashes = set((cutoff or {}).get("obsolete_secret_blob_hashes") or [])
    obsolete_secret_hashes.update(
        str(blob_hash)
        for operation in operations
        if str(operation.get("op") or "") == "CREDENTIAL_CUTOFF"
        for blob_hash in operation.get("obsolete_secret_blob_hashes") or []
    )
    for operation in operations:
        refs = _operation_hash_refs(operation)
        for manifest_hash in refs["config"]:
            for blob in _collect_config_manifest_blobs(cluster_root, operation, manifest_hash):
                config_by_hash.setdefault(str(blob["hash"]), blob)
        for payload_hash in refs["api_payload"]:
            raw = _read_local_blob(paths.config_blobs, payload_hash)
            config_by_hash.setdefault(payload_hash, {"hash": payload_hash, "raw": raw})
        for secret_hash in refs["secret"]:
            if secret_hash in obsolete_secret_hashes:
                continue
            raw = _read_local_blob(paths.secret_blobs, secret_hash)
            secret_by_hash.setdefault(secret_hash, {"hash": secret_hash, "raw": raw})
        for sealed_hash in refs["sealed"]:
            raw = _read_local_blob(paths.sealed_blobs, sealed_hash)
            _validate_sealed_blob_payload(cluster_root, raw)
            sealed_by_hash.setdefault(sealed_hash, {"hash": sealed_hash, "raw": raw})
    return (
        list(config_by_hash.values()),
        list(secret_by_hash.values()),
        list(sealed_by_hash.values()),
    )


def _collect_config_manifest_blobs(cluster_root: Path, operation: dict[str, Any], manifest_hash: str) -> list[dict[str, Any]]:
    """Collect a config manifest blob plus all file blobs, materializing local blobs if needed."""

    paths = ClusterPaths.from_root(cluster_root)
    try:
        manifest_raw = _read_local_blob(paths.config_blobs, manifest_hash)
    except OSError:
        manifest_raw = _build_local_config_blobs_from_instance(cluster_root, operation, manifest_hash)
    blobs = [{"hash": manifest_hash, "raw": manifest_raw}]
    for blob_hash in _manifest_file_hashes(manifest_raw):
        blobs.append({"hash": blob_hash, "raw": _read_local_blob(paths.config_blobs, blob_hash)})
    return blobs


def _build_local_config_blobs_from_instance(cluster_root: Path, operation: dict[str, Any], expected_hash: str) -> bytes:
    """Create local config blobs from the source run_v7 instance when available."""

    instance = str(operation.get("instance") or "")
    if not instance:
        raise ClusterSyncWorkerError("operation has no instance for config blob rebuild")
    instance_dir = Path(cluster_root).parent / "run_v7" / instance
    manifest = build_config_manifest(instance_dir)
    actual_hash = compute_config_manifest_hash(manifest)
    if actual_hash != expected_hash:
        raise ClusterSyncWorkerError(f"local config manifest for {instance} does not match desired state")
    manifest_raw = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
    paths = ClusterPaths.from_root(cluster_root)
    _write_local_blob(paths.config_blobs, expected_hash, manifest_raw, secret=False)
    files = manifest.get("files") if isinstance(manifest, dict) else {}
    files = files if isinstance(files, dict) else {}
    for filename, meta in files.items():
        sha = str((meta if isinstance(meta, dict) else {}).get("sha256") or "")
        if not sha:
            continue
        raw = (instance_dir / str(filename)).read_bytes()
        _write_local_blob(paths.config_blobs, f"sha256:{sha}", raw, secret=False)
    return manifest_raw


def _manifest_file_hashes(manifest_raw: bytes) -> list[str]:
    """Return file blob hashes referenced by a config manifest blob."""

    try:
        manifest = json.loads(manifest_raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ClusterSyncWorkerError("config manifest blob is not valid JSON") from exc
    files = manifest.get("files") if isinstance(manifest, dict) else {}
    files = files if isinstance(files, dict) else {}
    hashes: list[str] = []
    for meta in files.values():
        sha = str((meta if isinstance(meta, dict) else {}).get("sha256") or "")
        if sha:
            hashes.append(f"sha256:{sha}")
    return hashes


def _local_blob_path(base_dir: Path, blob_hash: str) -> Path:
    """Return the content-addressed path for one blob hash."""

    digest = _validate_hash(blob_hash).removeprefix("sha256:")
    return Path(base_dir) / "sha256" / digest[:2] / f"{digest}.json"


def _local_blob_exists(base_dir: Path, blob_hash: str) -> bool:
    """Return True if a verified local blob exists."""

    try:
        _read_local_blob(base_dir, blob_hash)
        return True
    except OSError:
        return False


def _read_local_blob(base_dir: Path, blob_hash: str) -> bytes:
    """Read and verify one local blob."""

    validated = _validate_hash(blob_hash)
    raw = _local_blob_path(base_dir, validated).read_bytes()
    if hashlib.sha256(raw).hexdigest() != validated.removeprefix("sha256:"):
        raise ClusterSyncWorkerError("local blob hash mismatch")
    return raw


def _write_local_blob(base_dir: Path, blob_hash: str, raw: bytes, *, secret: bool) -> None:
    """Write and verify one local content-addressed blob."""

    validated = _validate_hash(blob_hash)
    if hashlib.sha256(raw).hexdigest() != validated.removeprefix("sha256:"):
        raise ClusterSyncWorkerError("blob hash mismatch")
    path = _local_blob_path(base_dir, validated)
    if secret:
        ensure_private_directory_tree(Path(base_dir), path.parent)
        atomic_write_private_bytes(path, raw)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    tmp.write_bytes(raw)
    os.chmod(tmp, 0o600 if secret else 0o644)
    os.replace(tmp, path)


def _validate_hash(value: str) -> str:
    """Validate one sha256 hash string."""

    text = str(value or "")
    digest = text.removeprefix("sha256:")
    if not text.startswith("sha256:") or len(digest) != 64:
        raise ClusterSyncWorkerError("invalid blob hash")
    int(digest, 16)
    return text


def _is_unsupported_command_error(exc: Exception, command: str) -> bool:
    """Return True when an older peer rejected a newer wrapper command."""

    text = str(exc or "").lower()
    return "unsupported command" in text and str(command or "").lower() in text


def _remote_shell_path(path: str | None) -> str:
    """Return a POSIX shell expression for a remote path."""

    raw = str(path or "").strip().rstrip("/")
    if not raw or raw == "~":
        return '"$HOME"'
    if raw.startswith("~/"):
        suffix = raw[2:].strip("/")
        return '"$HOME"' + (f"/{shlex.quote(suffix)}" if suffix else "")
    if raw.startswith("/"):
        return shlex.quote(raw)
    return f'"$HOME"/{shlex.quote(raw)}'


def _remote_cluster_command(remote_pbgui_dir: str, local_node_id: str, command_text: str) -> str:
    """Build a remote direct wrapper command."""

    base = _remote_shell_path(remote_pbgui_dir or "software/pbgui")
    local_node = shlex.quote(str(local_node_id))
    return (
        f"base={base}; "
        "parent=\"${base%/*}\"; "
        "if [ -x \"$parent/venv_pbgui/bin/python\" ]; then py=\"$parent/venv_pbgui/bin/python\"; "
        "elif [ -x \"$parent/venv_pbgui312/bin/python\" ]; then py=\"$parent/venv_pbgui312/bin/python\"; "
        "elif [ -x \"$base/.venv/bin/python\" ]; then py=\"$base/.venv/bin/python\"; "
        "else py=python3; fi; "
        "\"$py\" \"$base/cluster_sync_command.py\" --cluster-root \"$base/data/cluster\" "
        f"--remote-node {local_node} --allow-join {command_text}"
    )
