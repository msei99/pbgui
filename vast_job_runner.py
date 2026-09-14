"""Independent local Vast rental guard and optimizer controller services."""

from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import re
import sys
import time

from logging_helpers import human_log as _log
from vast_credentials import VastCredentialStore
from vast_jobs import IMAGE, REVISION, PROJECT, JobStore, digest, job_id, write_json
from vast_provider import VastRateLimit, VastClient, VastError, positive_id
from vast_transfer import WorkerConnection, import_results

SERVICE = "VastRunner"


def validate_intent(intent: dict, identifier: str) -> dict:
    """Validate persisted authorization before provider or process operations."""
    if intent.get("id") != job_id(identifier) or intent.get("image") != IMAGE or intent.get("pb8_revision") != REVISION:
        raise VastError("Invalid cloud rental intent")
    if intent.get("label") != "pbgui-vast-" + identifier:
        raise VastError("Invalid cloud rental ownership")
    duration = float(intent["deadline"]) - float(intent["accepted_at"])
    if not 0 < duration <= 86400 or not .1 <= float(intent["budget_usd"]) <= 100:
        raise VastError("Invalid cloud rental limits")
    positive_id(intent["offer"]["id"])
    return intent


def rental_payload(intent: dict, registry_token: str = "") -> dict:
    """Use the pinned image worker without embedding code in provider arguments."""
    startup = ("set -e\numask 077\nmkdir -p /work/pbgui\ntouch /root/.no_auto_tmux\n"
               "cp /opt/pbgui/worker.py /work/pbgui/worker.py\n"
               "ssh-keygen -A\n"
               f"printf 'PBGUI_HOST_KEY {job_id(intent['id'])} %s\\n' "
               "\"$(cat /etc/ssh/ssh_host_ed25519_key.pub)\" > /proc/1/fd/1\n"
               "nohup /usr/local/bin/python /work/pbgui/worker.py guard > /work/pbgui/guard.log 2>&1 < /dev/null &\n")
    return {"client_id": "me", "image": IMAGE, "disk": intent["offer"]["disk_gb"],
            "label": intent["label"], "runtype": "ssh_direct", "target_state": "running", "cancel_unavail": True,
            "onstart": startup, "env": {"PBGUI_DEADLINE": str(intent["deadline"]), "PBGUI_JOB_ID": intent["id"]}}


def owned_instance(client: VastClient, intent: dict, *, fresh: bool = False) -> dict | None:
    """Only a full unique ownership label permits remote changes."""
    rows = client.instances(fresh=True) if fresh else client.instances()
    matches = [row for row in rows if row.get("label") == intent["label"]]
    if len(matches) > 1:
        raise VastError("Duplicate cloud ownership label; cleanup needs inspection")
    return matches[0] if matches else None


def guard_step(store: JobStore, identifier: str, client: VastClient, intent: dict, registry_token: str,
               *, now: float | None = None) -> bool:
    """Reconcile one lease step; creation is never blindly retried after timeout."""
    now = time.time() if now is None else now
    directory = store.directory(identifier)
    control = store.read(identifier, "control.json")
    state = store.read(identifier)
    ending = bool(control["cleanup"] or (control["stop"] and ((state.get("kind") != "worker" and not state.get("uploaded")) or (state.get("kind") == "worker" and not state.get("active_job"))))) or now >= intent["deadline"]
    marker = directory / "attempt.json"
    if not marker.exists() and not ending:
        write_json(marker, {"attempted_at": now})
        # This marker must be durable before the sole paid Create request.
        try:
            response = client.request("PUT", f"/asks/{intent['offer']['id']}/", rental_payload(intent, registry_token))
        except VastRateLimit as exc:
            if not exc.request_sent:
                marker.unlink()  # No paid request left this process; safe to try after cooldown.
            else:
                store.update(identifier, creation_error=str(exc))
            raise
        except VastError as exc:
            store.update(identifier, creation_error=str(exc))
            raise
        instance_id = positive_id(response.get("new_contract"))
        store.update(identifier, instance_id=instance_id)
        state = store.read(identifier)
    row = owned_instance(client, intent, fresh=ending)
    if row:
        if state.get("instance_id") and row["id"] != state["instance_id"]:
            raise VastError("Instance ID does not match the rental authorization")
        store.update(identifier, instance_id=row["id"], rental_state="destroy_pending" if ending else "active",
                     provider_status=str(row.get("actual_status") or "unknown")[:50], absent_checks=0, cleanup_error=None, creation_error=None)
        if ending:
            client.request("DELETE", f"/instances/{positive_id(row['id'])}")
        return False
    if ending:
        # Allow a timed-out creation to settle before accepting fresh absence
        # checks. A provider error never counts as proof of absence.
        attempted_at = store.read(identifier, 'attempt.json')['attempted_at'] if marker.exists() else intent['accepted_at']
        settle_until = min(intent['deadline'], attempted_at + 120)
        if marker.exists() and not state.get("instance_id") and now < settle_until:
            store.update(identifier, cleanup_wait_until=settle_until, provider_checked_at=now,
                         cleanup_error=None)
            if not state.get('cleanup_wait_until'):
                _log(SERVICE, f"Rental {identifier}: no instance found; awaiting ambiguous-create deadline "
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(settle_until))}", level='INFO')
            return False
        count = int(state.get("absent_checks", 0)) + 1
        store.update(identifier, absent_checks=count, cleanup_error=None, cleanup_wait_until=None,
                     provider_checked_at=now, rental_state="deletion_verified" if count >= 2 else "destroy_pending",
                     provider_status="deleted" if count >= 2 else "not_found")
        return count >= 2
    return False


def guard_loop(store: JobStore, identifier: str) -> None:
    """Run independently of SSH/downloads until provider absence is confirmed."""
    intent = validate_intent(store.read(identifier, "intent.json"), identifier)
    while True:
        try:
            secrets = VastCredentialStore(store.root).secrets()
            client = VastClient(secrets["api_key"])
            if guard_step(store, identifier, client, intent, ""):
                return
        except Exception as exc:
            safe_error = str(exc) if isinstance(exc, VastError) else "Rental reconciliation failed; retrying"
            _log(SERVICE, safe_error, level="WARNING")
            store.update(identifier, cleanup_error=safe_error)
        time.sleep(10)


def optimizer_started_at(connection) -> float:
    """Read the small worker-owned optimizer start record without changing the worker."""
    import math
    raw = connection.command('head -c 512 ' + connection.remote_root + '/started.json', max_output=512)
    try:
        value = json.loads(raw)['started_at']
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError('Invalid start time')
    except (ValueError, TypeError, KeyError):
        raise VastError('Optimizer start time unavailable') from None
    return float(value)


def finalize_collected_results(store: JobStore, identifier: str) -> dict:
    """Publish final results and assess their front without rewriting the stop cause."""
    from vast_convergence import observe
    imported = import_results(store, identifier)
    finished = json.loads((store.directory(identifier) / 'final-results/finished.json').read_text())
    state = store.read(identifier)
    reason = state.get('stop_reason')
    historical_convergence = reason == 'convergence' or (
        reason is None and bool(state.get('convergence', {}).get('stop_requested')))
    converged = historical_convergence and finished.get('cancelled') and finished.get('exit_code') in (0, -2, 130)
    status = 'cancelled' if finished.get('cancelled') else 'completed' if finished.get('exit_code') == 0 else 'failed'
    if converged:
        status = 'completed'
    store.update(identifier, **imported, exact_completed=imported.get('evaluations', state.get('exact_completed', 0)))
    observe(store, identifier, final=True)
    return store.update(identifier, status=status, error=None, exit_code=finished.get('exit_code'),
                        completion_reason='convergence' if converged else reason if reason != 'convergence' else None,
                        elapsed_seconds=finished.get('wall_seconds'), finished_at=finished.get('finished_at'))


def run_loop(store: JobStore, identifier: str, lease_id: str | None = None) -> None:
    """Reattach to a running worker and collect before the independent deadline."""
    from vast_convergence import DEFAULTS, observe
    row = store.read(identifier)
    if 'convergence_config' not in row:
        # Existing runs retain their original behavior after a supervisor update.
        store.update(identifier, convergence_config=dict(DEFAULTS))
    job_intent = store.read(identifier, "intent.json")
    intent = validate_intent(store.read(lease_id or identifier, "intent.json"), lease_id or identifier)
    directory = store.directory(identifier)
    last_backup = 0.0
    last_log = 0.0
    last_metrics = 0.0
    connection = None
    while True:
        state = store.read(identifier)
        if state.get("final_collected"):
            try:
                finalize_collected_results(store, identifier)
            except Exception:
                store.update(identifier, status="failed", error="Raw results saved locally; result import needs retry")
            if not lease_id:
                store.control(identifier, "cleanup")
            return
        if store.read(lease_id or identifier)["rental_state"] == "deletion_verified":
            if state["status"] not in {"completed", "failed", "cancelled"}:
                store.update(identifier, status="cancelled" if store.read(identifier, "control.json")["stop"] else "failed",
                             error="Rental ended; the last local snapshot remains available")
            return
        control = store.read(identifier, "control.json")
        if lease_id and store.read(lease_id, "control.json")["stop"]:
            store.control(identifier, "stop")
            control["stop"] = True
        if control["stop"] and not state.get("uploaded"):
            store.update(identifier, status="cancelled")
            if not lease_id:
                store.control(identifier, "cleanup")
            return
        if (not state.get("uploaded") and state.get("setup_started_at") is not None
                and time.time() > state["setup_started_at"] + 900):
            store.update(identifier, status="failed", error="Worker setup exceeded 15 minutes")
            if not lease_id:
                store.control(identifier, "cleanup")
            return
        remaining = intent["deadline"] - time.time()
        if remaining <= 15:
            if not lease_id:
                store.control(identifier, "cleanup")
            store.update(identifier, status="failed", error="Rental deadline reached; preserving last local snapshot")
            return
        try:
            client = VastClient(VastCredentialStore(store.root).secrets()["api_key"])
            row = owned_instance(client, intent)
            if row is not None and row.get("actual_status") == "running" and state.get("setup_started_at") is None:
                state = store.update(identifier, setup_started_at=time.time())
                _log(SERVICE, f"{identifier}: instance running; verifying SSH identity and preparing worker", level="INFO")
            if row is not None and row.get('actual_status') != 'running' and not state.get('worker_ready'):
                from vast_provisioning_log import collect_provisioning_log
                collect_provisioning_log(store, identifier, client, row['id'])
            if row is None or row.get("actual_status") != "running":
                time.sleep(5)
                continue
            connection = WorkerConnection(store, identifier, row, lease_id=lease_id)
            connection.bootstrap(client)
            control = store.read(identifier, "control.json")
            if control["cleanup"]:
                return
            if not state.get("worker_ready"):
                hardware = connection.operation("health", timeout=min(60, int(remaining)))
                guard = hardware.get("guard") or {}
                if hardware.get("revision") != REVISION or guard.get("instance_id") != row["id"] or guard.get("job_id") != (lease_id or identifier) or guard.get("deadline") != intent["deadline"]:
                    raise VastError("Worker identity or independent deadline guard mismatch", 422)
                if state.get('auto_cpu_workers'):
                    from vast_transfer import allocated_cpu_workers
                    state = store.update(identifier, workers=allocated_cpu_workers(hardware.get('cpu_cores')), cpu_allocation_resolved=True)
                if not state.get('auto_cpu_workers') and (hardware.get("cpu_cores") or 0) < state["workers"]:
                    raise VastError("Actual CPU quota is below the requested worker count", 422)
                store.update(identifier, worker_ready=True, hardware=hardware, status="uploading", error=None)
            if control["stop"] and not state.get("uploaded"):
                store.update(identifier, status="cancelled")
                if not lease_id:
                    store.control(identifier, "cleanup")
                return
            if not state.get("uploaded"):
                if digest(directory / "input.tar.gz") != job_intent["bundle_sha256"]:
                    raise VastError("Local input bundle changed after authorization", 422)
                attempts = int(state.get("upload_attempts", 0))
                if attempts >= 2:
                    raise VastError("Input upload failed twice; ending rental to limit transfer costs", 422)
                store.update(identifier, upload_attempts=attempts + 1)
                connection.upload(timeout=max(1, min(600, int(remaining - 180))))
                store.update(identifier, uploaded=True)
            progress = connection.operation("status")
            if not progress.get("started"):
                connection.start()
            elif not state.get('started_at'):
                store.update(identifier, started_at=optimizer_started_at(connection))
            if time.time() - last_metrics >= 15:
                from vast_runtime_metrics import sample_metrics
                sample_metrics(connection, store, identifier)
                last_metrics = time.time()
            if time.time() - last_log >= 15:
                from secure_files import atomic_write_private_text, ensure_private_directory
                from vast_jobs import PROJECT
                raw_log = connection.command('if test -f ' + connection.remote_root + '/output/optimizer.log; then tail -c 65536 '
                                             + connection.remote_root + '/output/optimizer.log; fi', max_output=65536)
                if raw_log:
                    from vast_exact_queue import parse_exact_queue
                    exact_queue = parse_exact_queue(raw_log)
                    if exact_queue is not None:
                        store.update(identifier, exact_queue=exact_queue)
                    log_root = ensure_private_directory(PROJECT / 'data/logs/optimizes_v8')
                    atomic_write_private_text(log_root / ('vast_' + identifier + '.log'), raw_log.decode('utf-8', errors='replace'))
                last_log = time.time()
            if control["stop"] or remaining < 240:
                if not state.get('stop_reason') and not progress.get('finished'):
                    reason = ('convergence' if state.get('convergence', {}).get('stop_requested') else 'requested') if control['stop'] else 'rental_deadline'
                    store.update(identifier, stop_reason=reason)
                connection.operation("stop")
                store.update(identifier, status="collecting")
            else:
                store.update(identifier, status="running")
                if lease_id:
                    store.update(lease_id, status="running")
            store.update(identifier, exact_completed=max(state.get("exact_completed", 0), progress.get("exact_completed", 0)),
                         gpu_candidates=max(state.get("gpu_candidates", 0), progress.get("gpu_candidates", 0)), error=None)
            if progress.get("finished"):
                store.update(identifier, status="collecting")
                connection.collect(True, timeout=max(1, min(180, int(remaining - 15))))
                continue
            if time.time() - last_backup >= 60 and progress.get("exact_completed", 0) > 0 and state.get("downloaded_bytes", 0) < 1024**3:
                connection.collect(False, timeout=max(1, min(60, int(remaining - 180))))
                store.update(identifier, **import_results(store, identifier, partial=True))
                observe(store, identifier)
                last_backup = time.time()
        except Exception as exc:
            safe_error = str(exc) if isinstance(exc, VastError) else "Worker operation failed; reconnecting"
            _log(SERVICE, safe_error, level="WARNING")
            store.update(identifier, error=safe_error)
            if isinstance(exc, VastError) and exc.status == 422:
                store.update(identifier, status="failed")
                if not lease_id:
                    store.control(identifier, "cleanup")
                return
        time.sleep(5)


def main() -> None:
    """Own one process lock per job role and let systemd recover unexpected exits."""
    os.umask(0o077)
    mode, identifier = sys.argv[1:]
    if mode not in {"guard", "run"}:
        raise ValueError("Invalid runner mode")
    store = JobStore()
    with (store.directory(identifier) / (mode + ".lock")).open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        if mode == "guard":
            guard_loop(store, identifier)
        else:
            if store.read(identifier).get("kind") == "worker":
                from vast_queue import worker_loop
                worker_loop(store, identifier)
            else:
                run_loop(store, identifier)


if __name__ == "__main__":
    main()
