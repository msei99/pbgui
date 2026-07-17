"""Lightweight PBCluster worker for local Cluster Sync reconciliation.

The worker owns local rebuild/materialization and will own peer fanout/pull in
the next phase. It intentionally does not start or stop bots; PBRun remains the
runtime gatekeeper and reads the local desired state.
"""

from __future__ import annotations

import json
import os
import base64
import binascii
import hashlib
import shlex
import subprocess
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Mapping

from cluster_sync_command import (
    ClusterSyncCommandError,
    MAX_BLOB_COVERAGE_HASHES,
    MAX_CONFIG_BLOB_BYTES,
    MAX_GET_OPS,
    MAX_SEALED_BLOB_BYTES,
    MAX_SECRET_BLOB_BYTES,
    _append_credential_migration_acks,
    _materialize_api_keys,
    _materialize_credentials,
    _materialize_v7_configs,
    _validate_sealed_blob_payload,
)
from cmc_leases import ClusterMailbox
from credential_reconciler import reconcile_pending_credentials
from credential_migration import (
    advance_local_credential_migration,
    credential_migration_is_complete,
    run_credential_migration,
)
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
    read_materialized_state,
    read_local_identity,
    rebuild_materialized_state,
    stage_membership_operations,
    validate_operation,
    V2_CREDENTIAL_OPS,
    write_operation,
)
from secure_files import atomic_write_private_bytes, ensure_private_directory_tree
from master.cluster_ssh_keys import ensure_cluster_ssh_key
from master.cluster_checkpoint import (
    ClusterCheckpointError,
    activate_checkpoint,
    active_checkpoint_bundle,
    build_shadow_checkpoint,
    checkpoint_status,
    create_checkpoint_ack,
    create_checkpoint_commit_proof,
    create_checkpoint_proposal,
    create_shadow_checkpoint,
    garbage_collect_blobs,
    install_rebootstrap_checkpoint,
    prune_operation_history,
    replica_blob_hashes,
    read_retention_report,
    read_shadow_checkpoint,
    retention_policy,
    retention_preview,
    verify_checkpoint_commit_proof,
)
from pbgui_purefunc import PBGDIR

SERVICE = "PBCluster"
STATUS_SCHEMA_VERSION = 1
DEFAULT_SSH_TIMEOUT = 30
CONFIG_BLOB_BATCH_TARGET_BYTES = 12 * 1024 * 1024
APPLY_BUNDLE_TARGET_BYTES = 12 * 1024 * 1024
APPLY_BUNDLE_MAX_OPERATIONS = 16
DEFAULT_PEER_WORKERS = 4
RETENTION_EVALUATION_SECONDS = 24 * 60 * 60
CHECKPOINT_OPERATION_TRIGGER = 5000
CHECKPOINT_BYTES_TRIGGER = 10 * 1024 * 1024
BLOB_COVERAGE_MAX_HASHES_PER_PEER_PASS = 16
BLOB_COVERAGE_MAX_BYTES_PER_PEER_PASS = 64 * 1024 * 1024
BLOB_COVERAGE_RECOVERY_SCAN_HASHES_PER_PEER_PASS = 64


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
        self._blob_coverage_cursors: dict[str, int] = {}
        self._blob_recovery_cursors: dict[str, int] = {}
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
        self._consume_trigger_change()
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
            self._consume_trigger_change()
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

        normalized_reason = str(reason or "manual")
        if normalized_reason == "event":
            self._peer_backoff.clear()
        scan_allowed = normalized_reason != "periodic"
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
                    scan_allowed=scan_allowed,
                )
            except Exception as exc:
                detail = str(exc)[:240]
                migration_pre_sync = {
                    "status": "error",
                    "error": type(exc).__name__,
                    "detail": detail,
                }
                _log(
                    SERVICE,
                    f"Credential migration advance pending: {type(exc).__name__}: {detail}",
                    level="WARNING",
                )
            materialized = read_materialized_state(self.cluster_root)
            peer_results = self._sync_peers(identity, materialized)
            if any(int(item.get("pulled_ops") or 0) for item in peer_results):
                materialized = rebuild_materialized_state(self.cluster_root)
            migration_coordinator = {"status": "not_coordinator"}
            if str(identity.get("role") or "").strip().lower() == "master":
                try:
                    coordinator_state = (
                        {"status": "complete", "phase": "complete"}
                        if normalized_reason != "boot"
                        and credential_migration_is_complete(self.pbgdir)
                        else run_credential_migration(self.pbgdir)
                    )
                    migration_coordinator = {
                        "status": str(coordinator_state.get("status") or "advanced"),
                        "phase": str(coordinator_state.get("phase") or "unknown"),
                    }
                    materialized = read_materialized_state(self.cluster_root)
                except Exception as exc:
                    detail = str(exc)[:240]
                    migration_coordinator = {
                        "status": "error",
                        "error": type(exc).__name__,
                        "detail": detail,
                    }
                    _log(
                        SERVICE,
                        f"Credential migration coordinator pending: {type(exc).__name__}: {detail}",
                        level="WARNING",
                    )
            v7_preview = _materialize_v7_configs(self.cluster_root, write=False)
            v7_result = v7_preview
            if v7_preview.get("can_apply"):
                v7_result = _materialize_v7_configs(self.cluster_root, write=True)
            api_preview = _materialize_api_keys(self.cluster_root, write=False)
            api_result = api_preview
            if api_preview.get("can_apply"):
                api_result = _materialize_api_keys(self.cluster_root, write=True)
            credential_preview = _materialize_credentials(self.cluster_root, write=False)
            credential_result = credential_preview
            if credential_preview.get("can_apply"):
                credential_result = _materialize_credentials(self.cluster_root, write=True)
            migration_ack = _append_credential_migration_acks(
                self.cluster_root,
                scan_allowed=False,
            )
            try:
                migration_post_sync = advance_local_credential_migration(
                    self.pbgdir,
                    max_items=8,
                    scan_allowed=False,
                )
            except Exception as exc:
                detail = str(exc)[:240]
                migration_post_sync = {
                    "status": "error",
                    "error": type(exc).__name__,
                    "detail": detail,
                }
                _log(
                    SERVICE,
                    f"Credential migration post-sync advance pending: {type(exc).__name__}: {detail}",
                    level="WARNING",
                )
            try:
                credential_reconciliation = reconcile_pending_credentials(self.pbgdir)
            except Exception as exc:
                detail = str(exc)[:240]
                credential_reconciliation = {
                    "status": "error",
                    "error": type(exc).__name__,
                    "detail": detail,
                }
                _log(
                    SERVICE,
                    f"Credential reconciliation pending: {type(exc).__name__}: {detail}",
                    level="WARNING",
                )

            history_materialized = read_materialized_state(self.cluster_root)
            try:
                history_retention = self._maintain_history(
                    identity,
                    history_materialized,
                    peer_results,
                    reason=normalized_reason,
                )
                if str(retention_policy(history_materialized).get("mode") or "") != "report_only":
                    materialized = read_materialized_state(self.cluster_root)
            except Exception as exc:
                detail = str(exc)[:240]
                history_retention = {
                    "status": "error",
                    "error": type(exc).__name__,
                    "detail": detail,
                }
                _log(SERVICE, f"Cluster history retention pending: {type(exc).__name__}: {detail}", level="WARNING")

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
                "history_retention": history_retention,
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

    def _maintain_history(
        self,
        identity: dict[str, Any],
        materialized: dict[str, Any],
        peer_results: list[dict[str, Any]],
        *,
        reason: str,
    ) -> dict[str, Any]:
        """Run bounded checkpoint/report/cleanup maintenance for one worker pass."""

        policy = retention_policy(materialized)
        now = int(time.time())
        active = checkpoint_status(self.cluster_root)
        current_vector = _as_state_vector(materialized.get("state_vector") or {})
        baseline = _as_state_vector(active.get("checkpoint_baseline") or {})
        report = read_retention_report(self.cluster_root)
        report_due = bool(
            reason in {"boot", "manual"}
            or not isinstance(report, dict)
            or int(report.get("evaluated_at") or 0) + RETENTION_EVALUATION_SECONDS <= now
            or str(report.get("mode") or "") != str(policy.get("mode") or "")
            or int(report.get("history_days") or 0) != int(policy.get("history_days") or 0)
        )
        result: dict[str, Any] = {
            "status": "idle",
            "policy": policy,
            "checkpoint": active,
            "report_due": report_due,
        }
        if str(policy.get("mode") or "report_only") == "report_only":
            try:
                shadow = read_shadow_checkpoint(self.cluster_root)
                shadow_baseline = _as_state_vector((shadow or {}).get("baseline_vector") or {})
            except (OSError, ClusterCheckpointError):
                shadow_baseline = {}
            shadow_tail = _checkpoint_tail_stats(self.cluster_root, shadow_baseline)
            result["checkpoint_tail"] = shadow_tail
            report_due = bool(
                report_due
                or int(shadow_tail["operations"]) >= CHECKPOINT_OPERATION_TRIGGER
                or int(shadow_tail["bytes"]) >= CHECKPOINT_BYTES_TRIGGER
            )
            result["report_due"] = report_due
            if report_due:
                checkpoint = create_shadow_checkpoint(self.cluster_root, created_at=now)
                preview = retention_preview(self.cluster_root, checkpoint, now=now, item_limit=0)
                scheduled_report = prune_operation_history(self.cluster_root, now=now, dry_run=True)
                result.update({
                    "status": "report_only",
                    "checkpoint_id": str(checkpoint["checkpoint_id"]),
                    "migration_seal": checkpoint["migration_seal"],
                    "eligible_operations": int(preview.get("eligible_operations") or 0),
                    "eligible_bytes": int(preview.get("eligible_bytes") or 0),
                    "blockers": list(scheduled_report.get("blockers") or []),
                })
            else:
                result["status"] = "report_only_cached"
            return result

        nodes = ((materialized.get("cluster_nodes") or {}).get("nodes") or {})
        local_node_id = str(identity.get("node_id") or "")
        active_masters = sorted(
            str(node_id)
            for node_id, node in nodes.items()
            if isinstance(node, dict)
            and str(node.get("role") or "") == "master"
            and node.get("enabled", True) is not False
            and node.get("state_replica", True) is not False
        )
        coordinator = active_masters[0] if active_masters else ""
        if current_vector != baseline:
            tail_stats = _checkpoint_tail_stats(self.cluster_root, baseline)
            result["checkpoint_tail"] = tail_stats
            checkpoint_due = bool(
                not active.get("active")
                or report_due
                or int(tail_stats["operations"]) >= CHECKPOINT_OPERATION_TRIGGER
                or int(tail_stats["bytes"]) >= CHECKPOINT_BYTES_TRIGGER
            )
            if not checkpoint_due:
                result["status"] = "checkpoint_tail_pending"
                return result
            if local_node_id != coordinator:
                result.update({"status": "awaiting_checkpoint_coordinator", "coordinator_id": coordinator})
                return result
            checkpoint_result = self._commit_checkpoint_clusterwide(
                identity,
                materialized,
                peer_results,
            )
            result["checkpoint_commit"] = checkpoint_result
            if checkpoint_result.get("status") != "committed":
                result["status"] = "checkpoint_blocked"
                return result
            materialized = read_materialized_state(self.cluster_root)
            result["checkpoint"] = checkpoint_status(self.cluster_root)

        if report_due:
            prune = prune_operation_history(self.cluster_root, now=now)
            result["oplog"] = prune
            if str(policy.get("mode") or "") == "oplog_and_blobs" and prune.get("status") == "complete":
                result["blobs"] = garbage_collect_blobs(self.cluster_root, now=now)
            result["status"] = "cleanup_evaluated"
        else:
            result["status"] = "cleanup_cached"
        return result

    def _commit_checkpoint_clusterwide(
        self,
        identity: dict[str, Any],
        materialized: dict[str, Any],
        peer_results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Collect exact replica ACKs and commit one checkpoint on every replica."""

        del peer_results
        checkpoint = build_shadow_checkpoint(self.cluster_root)
        seal = checkpoint.get("migration_seal") or {}
        if seal.get("status") != "sealed":
            return {"status": "blocked", "blockers": list(seal.get("blockers") or [])}
        now = int(time.time())
        proposal = create_checkpoint_proposal(
            self.cluster_root,
            checkpoint,
            created_at=now,
            expires_at=now + 300,
        )
        local_node_id = str(identity.get("node_id") or "")
        acknowledgements: list[dict[str, Any]] = [
            create_checkpoint_ack(self.cluster_root, checkpoint, proposal, created_at=now)
        ]
        nodes = ((materialized.get("cluster_nodes") or {}).get("nodes") or {})
        required = set(proposal.get("required_node_ids") or [])
        remote_ids = sorted(required - {local_node_id})
        unavailable = [
            node_id for node_id in remote_ids
            if not isinstance(nodes.get(node_id), dict)
            or normalize_node_sync_mode(nodes[node_id]) != "reachable"
            or not str(nodes[node_id].get("ssh_host") or "").strip()
        ]
        if unavailable:
            return {"status": "blocked", "blockers": [f"replica_unreachable:{node_id}" for node_id in unavailable]}
        prepare_payload = json.dumps(
            {"checkpoint": checkpoint, "proposal": proposal},
            sort_keys=True,
            separators=(",", ":"),
        )

        def prepare(node_id: str) -> dict[str, Any]:
            response = self.peer_client.run(
                nodes[node_id],
                local_node_id,
                "prepare-checkpoint",
                payload=prepare_payload,
            )
            ack = response.get("ack") if isinstance(response, dict) else None
            if not isinstance(ack, dict):
                raise ClusterSyncWorkerError(f"checkpoint ACK missing from {node_id}")
            return ack

        try:
            with ThreadPoolExecutor(
                max_workers=min(self.peer_workers, max(1, len(remote_ids))),
                thread_name_prefix="pbcluster-checkpoint-prepare",
            ) as executor:
                futures = {executor.submit(prepare, node_id): node_id for node_id in remote_ids}
                for future in as_completed(futures):
                    acknowledgements.append(future.result())
            proof = create_checkpoint_commit_proof(
                self.cluster_root,
                checkpoint,
                proposal,
                acknowledgements,
            )
            commit_payload = json.dumps(
                {"checkpoint": checkpoint, "commit_proof": proof},
                sort_keys=True,
                separators=(",", ":"),
            )

            def commit(node_id: str) -> None:
                self.peer_client.run(
                    nodes[node_id],
                    local_node_id,
                    "commit-checkpoint",
                    payload=commit_payload,
                )

            with ThreadPoolExecutor(
                max_workers=min(self.peer_workers, max(1, len(remote_ids))),
                thread_name_prefix="pbcluster-checkpoint-commit",
            ) as executor:
                futures = {executor.submit(commit, node_id): node_id for node_id in remote_ids}
                for future in as_completed(futures):
                    future.result()
            if _as_state_vector(read_materialized_state(self.cluster_root).get("state_vector") or {}) != _as_state_vector(checkpoint["baseline_vector"]):
                raise ClusterSyncWorkerError("local cluster state changed after checkpoint preparation")
            local_commit = activate_checkpoint(
                self.cluster_root,
                checkpoint,
                commit_proof=proof,
            )
            return {
                "status": "committed",
                "checkpoint_id": str(checkpoint["checkpoint_id"]),
                "epoch": int(local_commit["epoch"]),
                "acknowledgements": len(acknowledgements),
            }
        except Exception as exc:
            return {
                "status": "blocked",
                "blockers": [f"checkpoint_protocol:{type(exc).__name__}:{str(exc)[:160]}"],
            }

    def _sync_peers(self, identity: dict[str, Any], materialized: dict[str, Any]) -> list[dict[str, Any]]:
        """Synchronize with all currently known reachable peers."""

        cluster_nodes = materialized.get("cluster_nodes") if isinstance(materialized, dict) else {}
        nodes = cluster_nodes.get("nodes") if isinstance(cluster_nodes, dict) else {}
        nodes = nodes if isinstance(nodes, dict) else {}
        local_node_id = str(identity.get("node_id") or "")
        known_peer_ids = {str(node_id) for node_id in nodes if str(node_id) != local_node_id}
        for stale_peer_id in set(self._blob_coverage_cursors) - known_peer_ids:
            self._blob_coverage_cursors.pop(stale_peer_id, None)
        for stale_peer_id in set(self._blob_recovery_cursors) - known_peer_ids:
            self._blob_recovery_cursors.pop(stale_peer_id, None)
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
            allowed, reason = _peer_topology_allows(
                local_node_id,
                local_node,
                str(peer_id),
                peer,
                nodes,
            )
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
            local_vector = _as_state_vector(materialized.get("state_vector") or {})
            local_ops: list[dict[str, Any]] | None = None
            local_blob_hashes: dict[str, list[str]] | None = None
            local_ops_lock = threading.Lock()

            def load_local_ops() -> list[dict[str, Any]]:
                nonlocal local_ops
                with local_ops_lock:
                    if local_ops is None:
                        with self._local_state_lock:
                            local_ops = load_operations(
                                self.cluster_root,
                                expected_cluster_id=str(identity.get("cluster_id") or ""),
                            )
                    return local_ops

            def load_local_blob_hashes() -> dict[str, list[str]]:
                nonlocal local_blob_hashes
                with local_ops_lock:
                    if local_blob_hashes is None:
                        local_blob_hashes = replica_blob_hashes(self.cluster_root)
                    return local_blob_hashes

            max_workers = min(self.peer_workers, len(pending))
            with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="pbcluster-peer") as executor:
                future_map = {
                    executor.submit(
                        self._sync_peer_with_backoff,
                        peer_id,
                        peer,
                        local_node_id,
                        str(identity.get("cluster_id") or ""),
                        materialized,
                        load_local_ops,
                        load_local_blob_hashes,
                        local_vector,
                    ): peer_id
                    for peer_id, peer in pending
                }
                for future in as_completed(future_map):
                    peer_id = future_map[future]
                    results_by_peer[peer_id] = future.result()

        return [results_by_peer[str(peer_id)] for peer_id in sorted(nodes) if str(peer_id) != local_node_id and str(peer_id) in results_by_peer]

    def _sync_peer_with_backoff(
        self,
        peer_id: str,
        peer: dict[str, Any],
        local_node_id: str,
        cluster_id: str,
        local_materialized: dict[str, Any],
        load_local_ops: Callable[[], list[dict[str, Any]]],
        load_local_blob_hashes: Callable[[], dict[str, list[str]]],
        local_vector: dict[str, int],
    ) -> dict[str, Any]:
        """Synchronize one peer and update its retry backoff."""

        backoff = self._peer_backoff.get(str(peer_id)) or {}
        try:
            result = self._sync_peer(
                peer,
                local_node_id,
                cluster_id,
                local_materialized,
                load_local_ops,
                load_local_blob_hashes,
                local_vector,
            )
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

    def _sync_peer(
        self,
        peer: dict[str, Any],
        local_node_id: str,
        cluster_id: str,
        local_materialized: dict[str, Any],
        load_local_ops: Callable[[], list[dict[str, Any]]],
        load_local_blob_hashes: Callable[[], dict[str, list[str]]],
        local_vector: dict[str, int],
    ) -> dict[str, Any]:
        """Synchronize local state with one peer."""

        peer_id = str(peer.get("node_id") or "")
        base_result = _peer_result(peer_id, peer, ok=True, status="synced")
        hello, remote_vector = self._peer_handshake(peer, local_node_id)
        if str(hello.get("cluster_id") or "") != cluster_id:
            raise ClusterSyncWorkerError("peer belongs to another cluster")
        cutoff = (((local_materialized.get("desired_state") or {}).get("credential_migration") or {}).get("cutoff"))
        if isinstance(cutoff, dict) and int(hello.get("protocol_version") or 0) < int(cutoff.get("min_protocol") or 2):
            raise ClusterSyncWorkerError("credential protocol downgrade rejected after cutoff")
        remote_node_id = str(hello.get("node_id") or "")
        if peer_id and remote_node_id and remote_node_id != peer_id:
            raise ClusterSyncWorkerError("peer node_id does not match cluster_nodes")
        checkpoint_action = self._reconcile_peer_checkpoint(
            peer,
            local_node_id,
            hello,
            local_vector,
        )
        if checkpoint_action == "local_installed":
            local_materialized = read_materialized_state(self.cluster_root)
            local_vector = _as_state_vector(local_materialized.get("state_vector") or {})
        elif checkpoint_action == "remote_installed":
            remote_vector = _as_state_vector(
                checkpoint_status(self.cluster_root).get("checkpoint_baseline") or {}
            )
        with self._local_state_lock:
            key_changed = self._record_peer_cluster_ssh_metadata(peer, hello)

        pulled_ops = self._pull_missing_operations(peer, local_node_id, remote_vector, local_vector, cluster_id)

        remote_needs_operations = any(
            int(remote_vector.get(actor) or 0) < sequence
            for actor, sequence in local_vector.items()
        )
        push_ops = _select_operations_missing_on_remote(
            load_local_ops() if remote_needs_operations else [],
            remote_vector,
        )
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
        push_ops, remaining_push_ops = _bounded_operation_batch(push_ops)
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

        repaired_config_blobs = 0
        repaired_secret_blobs = 0
        repaired_sealed_blobs = 0
        recovered_local_blobs = 0
        blob_coverage_supported = bool(hello.get("blob_coverage_capability"))
        nodes = ((local_materialized.get("cluster_nodes") or {}).get("nodes") or {})
        active_masters = sorted(
            str(node_id)
            for node_id, node in nodes.items()
            if isinstance(node, dict)
            and str(node.get("role") or "") == "master"
            and node.get("enabled", True) is not False
            and node.get("state_replica", True) is not False
        )
        is_blob_repair_owner = local_node_id in active_masters
        if blob_coverage_supported and is_blob_repair_owner:
            repaired_config_blobs, repaired_secret_blobs, repaired_sealed_blobs, recovered_local_blobs = self._repair_remote_blob_coverage(
                peer,
                local_node_id,
                load_local_blob_hashes(),
            )

        mailbox_counts = {"supported": False, "pulled": 0, "pushed": 0, "acked": 0}
        if bool(hello.get("mailbox_capability")):
            with self._local_state_lock:
                mailbox_materialized = rebuild_materialized_state(
                    self.cluster_root,
                    write=False,
                )
            mailbox_nodes = (mailbox_materialized.get("cluster_nodes") or {}).get("nodes") or {}
            mailbox_counts = self._sync_mailbox(
                peer,
                local_node_id,
                membership_nodes=mailbox_nodes,
            )

        if (
            pulled_ops
            or pushed_ops
            or repaired_config_blobs
            or repaired_secret_blobs
            or repaired_sealed_blobs
            or recovered_local_blobs
            or key_changed
            or mailbox_counts["pulled"]
            or mailbox_counts["pushed"]
        ):
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
            "blob_coverage_supported": blob_coverage_supported,
            "blob_coverage_repair_owner": is_blob_repair_owner,
            "repaired_config_blobs": repaired_config_blobs,
            "repaired_secret_blobs": repaired_secret_blobs,
            "repaired_sealed_blobs": repaired_sealed_blobs,
            "recovered_local_blobs": recovered_local_blobs,
            "deferred_credential_ops": deferred_credential_ops,
            "remaining_push_ops": remaining_push_ops,
            "cluster_ssh_key_updated": key_changed,
            "mailbox_supported": mailbox_counts["supported"],
            "mailbox_pulled": mailbox_counts["pulled"],
            "mailbox_pushed": mailbox_counts["pushed"],
            "mailbox_acked": mailbox_counts["acked"],
            "checkpoint_action": checkpoint_action,
            "last_seen": int(time.time()),
        })
        return base_result

    def _reconcile_peer_checkpoint(
        self,
        peer: dict[str, Any],
        local_node_id: str,
        hello: dict[str, Any],
        local_vector: dict[str, int],
    ) -> str:
        """Install the newer proven checkpoint before comparing operation tails."""

        local = checkpoint_status(self.cluster_root)
        local_id = str(local.get("checkpoint_id") or "")
        remote_id = str(hello.get("checkpoint_id") or "")
        if local_id == remote_id:
            return "current" if local_id else "none"
        local_baseline = _as_state_vector(local.get("checkpoint_baseline") or {})
        remote_baseline = _as_state_vector(hello.get("checkpoint_baseline") or {})
        actors = set(local_baseline) | set(remote_baseline)
        remote_dominates = all(remote_baseline.get(actor, 0) >= local_baseline.get(actor, 0) for actor in actors)
        local_dominates = all(local_baseline.get(actor, 0) >= remote_baseline.get(actor, 0) for actor in actors)
        if local_id and remote_id and (not remote_dominates and not local_dominates):
            raise ClusterSyncWorkerError("peer checkpoint baselines are incomparable")
        if local_id and remote_id and local_baseline == remote_baseline:
            raise ClusterSyncWorkerError("peer reports a different checkpoint for the same baseline")
        if remote_id and (not local_id or remote_dominates):
            payload = self.peer_client.run(peer, local_node_id, "get-checkpoint-state")
            bundle = payload.get("bundle") if isinstance(payload, dict) else None
            checkpoint = bundle.get("checkpoint") if isinstance(bundle, dict) else None
            proof = bundle.get("commit_proof") if isinstance(bundle, dict) else None
            if not isinstance(checkpoint, dict) or not isinstance(proof, dict):
                raise ClusterSyncWorkerError("peer checkpoint bundle is incomplete")
            if str(checkpoint.get("checkpoint_id") or "") != remote_id:
                raise ClusterSyncWorkerError("peer checkpoint bundle ID mismatch")
            verify_checkpoint_commit_proof(checkpoint, proof)
            self._pull_checkpoint_blobs(peer, local_node_id, checkpoint)
            install_rebootstrap_checkpoint(self.cluster_root, checkpoint, proof)
            return "local_installed"
        if local_id and (not remote_id or local_dominates):
            bundle = active_checkpoint_bundle(self.cluster_root)
            if not isinstance(bundle, dict):
                raise ClusterSyncWorkerError("local checkpoint bundle is unavailable")
            checkpoint = bundle["checkpoint"]
            proof = bundle["commit_proof"]
            self._push_checkpoint_blobs(peer, local_node_id, checkpoint)
            payload = json.dumps(
                {"checkpoint": checkpoint, "commit_proof": proof},
                sort_keys=True,
                separators=(",", ":"),
            )
            self.peer_client.run(peer, local_node_id, "install-checkpoint", payload=payload)
            return "remote_installed"
        raise ClusterSyncWorkerError("checkpoint negotiation could not select a safe direction")

    def _pull_checkpoint_blobs(
        self,
        peer: dict[str, Any],
        local_node_id: str,
        checkpoint: dict[str, Any],
    ) -> None:
        """Pull every direct and manifest-expanded blob needed by a checkpoint."""

        refs = checkpoint.get("blob_refs") or {}
        paths = ClusterPaths.from_root(self.cluster_root)
        for blob_hash in refs.get("config") or []:
            raw = self._ensure_remote_blob(peer, local_node_id, paths.config_blobs, str(blob_hash), secret=False)
            manifest_raw = raw if raw is not None else _read_local_blob(paths.config_blobs, str(blob_hash))
            for child_hash in _manifest_file_hashes(manifest_raw):
                self._ensure_remote_blob(peer, local_node_id, paths.config_blobs, child_hash, secret=False)
        for blob_hash in refs.get("secret") or []:
            self._ensure_remote_blob(peer, local_node_id, paths.secret_blobs, str(blob_hash), secret=True)
        for blob_hash in refs.get("sealed") or []:
            self._ensure_remote_blob(
                peer,
                local_node_id,
                paths.sealed_blobs,
                str(blob_hash),
                secret=True,
                sealed=True,
            )

    def _push_checkpoint_blobs(
        self,
        peer: dict[str, Any],
        local_node_id: str,
        checkpoint: dict[str, Any],
    ) -> None:
        """Push every checkpoint-reachable blob before remote rebootstrap."""

        refs = checkpoint.get("blob_refs") or {}
        paths = ClusterPaths.from_root(self.cluster_root)
        config_hashes = {str(item) for item in refs.get("config") or []}
        for manifest_hash in list(config_hashes):
            for child_hash in _manifest_file_hashes(_read_local_blob(paths.config_blobs, manifest_hash)):
                config_hashes.add(child_hash)
        config_blobs = [
            {"hash": blob_hash, "raw": _read_local_blob(paths.config_blobs, blob_hash)}
            for blob_hash in sorted(config_hashes)
        ]
        for chunk in _chunk_config_blobs(config_blobs):
            self.peer_client.run(peer, local_node_id, "put-blobs", payload=_blob_batch_payload(chunk))
        for blob_hash in refs.get("secret") or []:
            raw = _read_local_blob(paths.secret_blobs, str(blob_hash))
            self.peer_client.run(peer, local_node_id, f"put-secret-blob {shlex.quote(str(blob_hash))}", payload=raw)
        for blob_hash in refs.get("sealed") or []:
            raw = _read_local_blob(paths.sealed_blobs, str(blob_hash))
            self.peer_client.run(peer, local_node_id, f"put-sealed-blob {shlex.quote(str(blob_hash))}", payload=raw)

    def _sync_mailbox(
        self,
        peer: dict[str, Any],
        local_node_id: str,
        *,
        membership_nodes: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
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
        mailbox = ClusterMailbox(self.cluster_root, membership_nodes=membership_nodes)
        local_ids = {str(item["message_id"]) for item in mailbox.index()}

        for message_id in sorted(remote_ids - local_ids):
            try:
                payload = self.peer_client.run(
                    peer,
                    local_node_id,
                    f"get-mailbox-message {shlex.quote(message_id)}",
                )
            except Exception as exc:
                error = str(exc or "").lower()
                if "mailbox message not found" in error or "mailbox message has expired" in error:
                    continue
                raise
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
        remaining_bytes: int | None = None,
    ) -> bytes | None:
        """Ensure one local blob exists by pulling it from the peer when missing."""

        if _local_blob_exists(base_dir, blob_hash):
            return None
        expected_hash = _validate_hash(blob_hash)
        verb = "get-sealed-blob" if sealed else "get-secret-blob" if secret else "get-blob"
        payload = self.peer_client.run(peer, local_node_id, f"{verb} {shlex.quote(expected_hash)}")
        if not isinstance(payload.get("hash"), str):
            raise ClusterSyncWorkerError("peer returned invalid blob hash type")
        reported_hash = _validate_hash(payload["hash"])
        if reported_hash != expected_hash:
            raise ClusterSyncWorkerError("peer returned a different blob hash")
        if type(payload.get("size")) is not int:
            raise ClusterSyncWorkerError("peer returned invalid blob size type")
        reported_size = int(payload["size"])
        if reported_size < 0:
            raise ClusterSyncWorkerError("peer returned invalid blob size")
        max_size = MAX_SEALED_BLOB_BYTES if sealed else MAX_SECRET_BLOB_BYTES if secret else MAX_CONFIG_BLOB_BYTES
        if reported_size > max_size:
            raise ClusterSyncWorkerError("peer blob exceeds type size limit")
        if remaining_bytes is not None and reported_size > max(0, int(remaining_bytes)):
            raise ClusterBlobBudgetExceeded("peer blob exceeds coverage pass budget")
        content_b64 = payload.get("content_b64")
        if not isinstance(content_b64, str):
            raise ClusterSyncWorkerError("peer returned invalid blob encoding type")
        if len(content_b64) > ((reported_size + 2) // 3) * 4:
            raise ClusterSyncWorkerError("peer blob encoding exceeds declared size")
        try:
            raw = base64.b64decode(content_b64, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ClusterSyncWorkerError("peer returned invalid blob encoding") from exc
        if reported_size != len(raw):
            raise ClusterSyncWorkerError("peer blob size does not match content")
        if sealed:
            _validate_sealed_blob_payload(self.cluster_root, raw)
        _write_local_blob(base_dir, expected_hash, raw, secret=secret)
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

    def _repair_remote_blob_coverage(
        self,
        peer: dict[str, Any],
        local_node_id: str,
        hashes_by_kind: dict[str, list[str]],
    ) -> tuple[int, int, int, int]:
        """Exchange replica-relevant blobs missing on either converged peer."""

        expected_kinds = ("config", "secret", "sealed")
        by_kind = {kind: sorted({_validate_hash(item) for item in hashes_by_kind.get(kind) or []}) for kind in expected_kinds}
        all_refs = [(kind, blob_hash) for kind in expected_kinds for blob_hash in by_kind[kind]]
        peer_id = str(peer.get("node_id") or "")
        if not all_refs:
            self._blob_coverage_cursors.pop(peer_id, None)
            self._blob_recovery_cursors.pop(peer_id, None)
            return 0, 0, 0, 0
        paths = ClusterPaths.from_root(self.cluster_root)
        roots = {
            "config": paths.config_blobs,
            "secret": paths.secret_blobs,
            "sealed": paths.sealed_blobs,
        }
        recovery_start = int(self._blob_recovery_cursors.get(peer_id) or 0) % len(all_refs)
        recovery_scan = [
            all_refs[(recovery_start + offset) % len(all_refs)]
            for offset in range(min(BLOB_COVERAGE_RECOVERY_SCAN_HASHES_PER_PEER_PASS, len(all_refs)))
        ]
        self._blob_recovery_cursors[peer_id] = (recovery_start + len(recovery_scan)) % len(all_refs)
        locally_absent = [
            (kind, blob_hash)
            for kind, blob_hash in recovery_scan
            if (
                _local_blob_path(roots[kind], blob_hash).is_symlink()
                or not _local_blob_path(roots[kind], blob_hash).is_file()
            )
        ]
        start = int(self._blob_coverage_cursors.get(peer_id) or 0) % len(all_refs)
        selected = [
            all_refs[(start + offset) % len(all_refs)]
            for offset in range(min(BLOB_COVERAGE_MAX_HASHES_PER_PEER_PASS, len(all_refs)))
        ]

        def probe(kind: str, requested: list[str], target: dict[str, set[str]]) -> None:
            request = {name: requested if name == kind else [] for name in expected_kinds}
            try:
                response = self.peer_client.run(
                    peer,
                    local_node_id,
                    "missing-blobs",
                    payload=json.dumps(request, sort_keys=True, separators=(",", ":")),
                )
            except Exception as exc:
                if len(requested) <= 1 or "missing-blobs verification budget exceeded" not in str(exc).lower():
                    raise
                midpoint = len(requested) // 2
                probe(kind, requested[:midpoint], target)
                probe(kind, requested[midpoint:], target)
                return
            reported = (response.get("missing") or {}).get(kind)
            if not isinstance(reported, list):
                raise ClusterSyncWorkerError("peer missing-blobs response is invalid")
            normalized = [_validate_hash(item) for item in reported]
            if normalized != sorted(set(normalized)) or not set(normalized).issubset(requested):
                raise ClusterSyncWorkerError("peer missing-blobs response contains unexpected hashes")
            target[kind].update(normalized)

        recovery_candidates = locally_absent[:BLOB_COVERAGE_MAX_HASHES_PER_PEER_PASS]
        recovery_missing: dict[str, set[str]] = {kind: set() for kind in expected_kinds}
        recovery_by_kind = {
            kind: sorted(blob_hash for candidate_kind, blob_hash in recovery_candidates if candidate_kind == kind)
            for kind in expected_kinds
        }
        for kind, hashes in recovery_by_kind.items():
            if hashes:
                probe(kind, hashes, recovery_missing)

        recovered_local = 0
        repair_bytes = 0
        for kind, blob_hash in recovery_candidates:
            if blob_hash in recovery_missing[kind] or repair_bytes >= BLOB_COVERAGE_MAX_BYTES_PER_PEER_PASS:
                continue
            try:
                raw = self._ensure_remote_blob(
                    peer,
                    local_node_id,
                    roots[kind],
                    blob_hash,
                    secret=kind != "config",
                    sealed=kind == "sealed",
                    remaining_bytes=BLOB_COVERAGE_MAX_BYTES_PER_PEER_PASS - repair_bytes,
                )
            except ClusterBlobBudgetExceeded:
                break
            if raw is not None:
                repair_bytes += len(raw)
                recovered_local += 1

        missing: dict[str, set[str]] = {kind: set() for kind in expected_kinds}
        selected_by_kind = {
            kind: sorted(blob_hash for selected_kind, blob_hash in selected if selected_kind == kind)
            for kind in expected_kinds
        }
        for kind, hashes in selected_by_kind.items():
            for offset in range(0, len(hashes), MAX_BLOB_COVERAGE_HASHES):
                requested = hashes[offset:offset + MAX_BLOB_COVERAGE_HASHES]
                probe(kind, requested, missing)

        unavailable = 0
        config_blobs = []
        pushed_secret = 0
        pushed_sealed = 0
        processed = 0
        for kind, blob_hash in selected:
            if blob_hash not in missing[kind]:
                if repair_bytes >= BLOB_COVERAGE_MAX_BYTES_PER_PEER_PASS:
                    break
                try:
                    raw = self._ensure_remote_blob(
                        peer,
                        local_node_id,
                        roots[kind],
                        blob_hash,
                        secret=kind != "config",
                        sealed=kind == "sealed",
                        remaining_bytes=BLOB_COVERAGE_MAX_BYTES_PER_PEER_PASS - repair_bytes,
                    )
                except ClusterBlobBudgetExceeded:
                    break
                if raw is not None:
                    repair_bytes += len(raw)
                    recovered_local += 1
                processed += 1
                continue
            try:
                if kind == "config":
                    raw = _read_local_blob(paths.config_blobs, blob_hash)
                elif kind == "secret":
                    raw = _read_local_blob(paths.secret_blobs, blob_hash)
                else:
                    raw = _read_local_blob(paths.sealed_blobs, blob_hash)
                    _validate_sealed_blob_payload(self.cluster_root, raw)
            except (OSError, ClusterSyncWorkerError, ClusterSyncCommandError):
                unavailable += 1
                processed += 1
                continue
            if len(raw) > BLOB_COVERAGE_MAX_BYTES_PER_PEER_PASS:
                unavailable += 1
                processed += 1
                continue
            if repair_bytes + len(raw) > BLOB_COVERAGE_MAX_BYTES_PER_PEER_PASS:
                break
            repair_bytes += len(raw)
            if kind == "config":
                config_blobs.append({"hash": blob_hash, "raw": raw})
            elif kind == "secret":
                self.peer_client.run(peer, local_node_id, f"put-secret-blob {shlex.quote(blob_hash)}", payload=raw)
                pushed_secret += 1
            else:
                self.peer_client.run(peer, local_node_id, f"put-sealed-blob {shlex.quote(blob_hash)}", payload=raw)
                pushed_sealed += 1
            processed += 1
        for chunk in _chunk_config_blobs(config_blobs):
            self.peer_client.run(peer, local_node_id, "put-blobs", payload=_blob_batch_payload(chunk))
        self._blob_coverage_cursors[peer_id] = (start + processed) % len(all_refs)
        if unavailable:
            _log(SERVICE, f"Skipped {unavailable} locally unavailable blob repairs for {peer.get('pbname') or peer.get('node_id')}", level="WARNING")
        return len(config_blobs), pushed_secret, pushed_sealed, recovered_local

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


class ClusterBlobBudgetExceeded(ClusterSyncWorkerError):
    """Raised when one valid blob cannot fit in the remaining repair budget."""


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


def _peer_topology_allows(
    local_node_id: str,
    local_node: dict[str, Any],
    peer_id: str,
    peer: dict[str, Any],
    nodes: dict[str, dict[str, Any]],
) -> tuple[bool, str]:
    """Return whether this node should actively contact *peer* by default."""

    local_role = str((local_node or {}).get("role") or "").strip()
    if local_role == "master":
        coordinator_id = min(
            (
                str(node_id)
                for node_id, node in nodes.items()
                if isinstance(node, dict)
                and node.get("enabled", True) is not False
                and node.get("state_replica", True) is not False
                and str(node.get("role") or "").strip() == "master"
            ),
            default="",
        )
        if coordinator_id and str(local_node_id) != coordinator_id:
            if str(peer_id) == coordinator_id:
                return True, "secondary master syncs through coordinator"
            return False, "secondary master fanout is delegated to coordinator"

    explicit = local_node.get("sync_peers") if isinstance(local_node, dict) else None
    if isinstance(explicit, list):
        if str(peer_id) in {str(item) for item in explicit}:
            return True, "explicit sync peer"
        return False, "peer is not in sync_peers"
    if local_role == "vps":
        return False, "VPS nodes do not initiate peer SSH without explicit sync_peers"
    return True, ""


def _bounded_operation_batch(operations: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Return one bounded transport batch and the number left for later passes."""

    return operations[:APPLY_BUNDLE_MAX_OPERATIONS], max(len(operations) - APPLY_BUNDLE_MAX_OPERATIONS, 0)


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
        "blob_coverage_supported": False,
        "blob_coverage_repair_owner": False,
        "repaired_config_blobs": 0,
        "repaired_secret_blobs": 0,
        "repaired_sealed_blobs": 0,
        "recovered_local_blobs": 0,
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


def _checkpoint_tail_stats(cluster_root: Path, baseline: Mapping[str, int]) -> dict[str, int]:
    """Return operation count and bytes strictly above one checkpoint baseline."""

    oplog = ClusterPaths.from_root(cluster_root).oplog
    operations = 0
    size = 0
    if not oplog.exists():
        return {"operations": 0, "bytes": 0}
    for actor_dir in (path for path in oplog.iterdir() if path.is_dir()):
        actor_baseline = int(baseline.get(str(actor_dir.name), 0))
        for path in actor_dir.glob("*.json"):
            try:
                if int(path.stem) <= actor_baseline:
                    continue
                size += int(path.stat().st_size)
            except (OSError, ValueError) as exc:
                raise ClusterSyncWorkerError("checkpoint tail contains an invalid operation file") from exc
            operations += 1
    return {"operations": operations, "bytes": size}


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
    except (OSError, ClusterSyncWorkerError):
        return False


def _read_local_blob(base_dir: Path, blob_hash: str) -> bytes:
    """Read and verify one local blob."""

    validated = _validate_hash(blob_hash)
    path = _local_blob_path(base_dir, validated)
    _reject_blob_path_symlinks(base_dir, path)
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != validated.removeprefix("sha256:"):
        raise ClusterSyncWorkerError("local blob hash mismatch")
    return raw


def _write_local_blob(base_dir: Path, blob_hash: str, raw: bytes, *, secret: bool) -> None:
    """Write and verify one local content-addressed blob."""

    validated = _validate_hash(blob_hash)
    if hashlib.sha256(raw).hexdigest() != validated.removeprefix("sha256:"):
        raise ClusterSyncWorkerError("blob hash mismatch")
    path = _local_blob_path(base_dir, validated)
    _reject_blob_path_symlinks(base_dir, path)
    if secret:
        ensure_private_directory_tree(Path(base_dir), path.parent)
        atomic_write_private_bytes(path, raw)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    tmp.write_bytes(raw)
    os.chmod(tmp, 0o600 if secret else 0o644)
    os.replace(tmp, path)


def _reject_blob_path_symlinks(base_dir: Path, path: Path) -> None:
    """Reject symlinks at every fixed content-addressed store boundary."""

    base = Path(base_dir)
    for candidate in (base, base / "sha256", Path(path).parent, Path(path)):
        if candidate.is_symlink():
            raise ClusterSyncWorkerError("blob store path must not contain symlinks")


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
