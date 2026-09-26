"""Independent local Vast rental guard and optimizer controller services."""

from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import re
import sys
import time
import traceback

import psutil

from logging_helpers import human_log as _log
from vast_credentials import VastCredentialStore
from vast_jobs import SUPPORTED_RENTAL_IMAGES, SUPPORTED_RENTAL_IMAGE_REVISIONS, PROJECT, JobStore, digest, job_id, write_json
from vast_provider import VastRateLimit, VastClient, VastError, positive_id
from vast_transfer import WorkerConnection, import_results

SERVICE = "VastRunner"
RUNNER_CODE_SERIAL = (PROJECT / "api/serial.txt").read_text().strip()
UPLOAD_RECOVERY_SECONDS = 15 * 60
UPLOAD_FINISH_RESERVE_SECONDS = 180


def schedule_upload_retry(store: JobStore, identifier: str, exc: Exception, deadline: float) -> None:
    """Persist a bounded recovery window, retaining resumable transfer state."""
    from pb8_config import PB8RuntimeBusyError

    runtime_busy = isinstance(exc, PB8RuntimeBusyError)
    if not runtime_busy and not isinstance(exc, VastError):
        raise VastError('Input upload failed unexpectedly; inspect the local worker log', 422) from None
    if isinstance(exc, VastError) and exc.status != 502:
        raise exc
    reason = str(exc)
    # These diagnoses cannot recover by waiting and must never bypass validation.
    if any(token in reason.lower() for token in ('host identity verification failed',
            'host key verification failed', 'remote disk full', 'no space left',
            'invalid ', 'checksum', 'exceeds the permitted')):
        raise VastError(reason, 422) from None
    transient = runtime_busy or any(token in reason.lower() for token in ('timed out', 'timeout',
        'connection', 'interrupted', 'reconnecting', 'stalled', 'hostname resolution',
        'remote rsync or ssh operation failed',
        'authentication or permission denied'))
    if not transient:
        raise VastError(reason, 422) from None
    now = time.time()
    state = store.read(identifier)
    progress = state.get('upload_progress') or {}
    count = max((progress.get(key) or 0 for key in ('bytes', 'transferred_bytes')), default=0)
    previous = state.get('upload_retry_bytes', 0)
    since = state.get('upload_recovery_since')
    if since is None or count > previous:
        since = now
    if now >= deadline - UPLOAD_FINISH_RESERVE_SECONDS:
        raise VastError('Input upload could not finish before the rental collection reserve; partial data retained', 422)
    if now - since >= UPLOAD_RECOVERY_SECONDS:
        raise VastError('Input upload could not recover for 15 minutes without new transfer progress; last error: ' + reason, 422)
    failures = state.get('upload_retry_failures', 0) + 1
    delay = min(60, 15 * 2 ** min(failures - 1, 2))
    retry_at = min(now + delay, since + UPLOAD_RECOVERY_SECONDS, deadline - UPLOAD_FINISH_RESERVE_SECONDS)
    progress = dict(progress, stage='reconnecting', bytes_per_second=0)
    store.update(identifier, upload_recovery_since=since, upload_retry_bytes=max(count, previous),
                 upload_retry_failures=failures, upload_retry_at=retry_at,
                 upload_progress=progress, status='uploading',
                 error=f'Upload connection interrupted; retrying in {max(1, int(retry_at - now))}s '
                       f'(up to 15 minutes to recover; partial data retained). Last error: {reason}')


def sync_optimizer_log(connection, store: JobStore, identifier: str) -> None:
    """Bound telemetry independently so log failures cannot skip stop or collection."""
    from secure_files import atomic_write_private_text, ensure_private_directory
    from vast_jobs import PROJECT
    try:
        raw_log = connection.command('if test -f ' + connection.remote_root + '/output/optimizer.log; then tail -c 65536 '
                                     + connection.remote_root + '/output/optimizer.log; fi', max_output=131072)
        raw_log = raw_log[-65536:]
        if raw_log:
            from vast_exact_queue import parse_exact_queue
            exact_queue = parse_exact_queue(raw_log)
            if exact_queue is not None:
                store.update(identifier, exact_queue=exact_queue)
            log_root = ensure_private_directory(PROJECT / 'data/logs/optimizes_v8')
            atomic_write_private_text(log_root / ('vast_' + identifier + '.log'), raw_log.decode('utf-8', errors='replace'))
        store.update(identifier, log_error=None)
    except Exception as exc:
        # Never publish arbitrary remote payloads or exception values.
        error = str(exc) if isinstance(exc, VastError) else 'Optimizer log synchronization failed'
        _log(SERVICE, error, level='WARNING')
        store.update(identifier, log_error=error)


def validate_intent(intent: dict, identifier: str) -> dict:
    """Validate persisted authorization before provider or process operations."""
    if (intent.get("id") != job_id(identifier)
            or not isinstance(intent.get("image"), str)
            or intent.get("pb8_revision") != SUPPORTED_RENTAL_IMAGE_REVISIONS.get(intent.get("image"))):
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
    from vast_deadline import maximum_deadline
    if intent.get("image") not in SUPPORTED_RENTAL_IMAGES:
        raise VastError("Invalid cloud rental image")
    startup = ("set -e\numask 077\nmkdir -p /work/pbgui\ntouch /root/.no_auto_tmux\n"
               "cp /opt/pbgui/worker.py /work/pbgui/worker.py\n"
               "ssh-keygen -A\n"
               f"printf 'PBGUI_HOST_KEY {job_id(intent['id'])} %s\\n' "
               "\"$(cat /etc/ssh/ssh_host_ed25519_key.pub)\" > /proc/1/fd/1\n"
               "nohup /usr/local/bin/python /work/pbgui/worker.py guard > /work/pbgui/guard.log 2>&1 < /dev/null &\n")
    return {"client_id": "me", "image": intent["image"], "disk": intent["offer"]["disk_gb"],
            "label": intent["label"], "runtype": "ssh_direct", "target_state": "running", "cancel_unavail": True,
            "onstart": startup, "env": {"PBGUI_DEADLINE": str(intent["deadline"]), "PBGUI_JOB_ID": intent["id"], "PBGUI_MAX_DEADLINE": str(maximum_deadline(intent)), "PBGUI_HARD_DEADLINE": str(intent['accepted_at'] + 86400 if 'accepted_at' in intent else maximum_deadline(intent))}}


def owned_instance(client: VastClient, intent: dict, *, fresh: bool = False,
                   expected_id: int | None = None) -> dict | None:
    """Only a full unique ownership label permits remote changes."""
    rows = client.instances(fresh=True) if fresh else client.instances()
    if expected_id is not None:
        existing = [row for row in rows if row['id'] == expected_id]
        if any(row.get('label') != intent['label'] for row in existing):
            raise VastError("Rental instance still exists but its ownership label changed; cleanup needs inspection")
    matches = [row for row in rows if row.get("label") == intent["label"]]
    if len(matches) > 1:
        raise VastError("Duplicate cloud ownership label; cleanup needs inspection")
    return matches[0] if matches else None


def uncreated_rental_message(state: dict) -> str | None:
    """Explain a rejected/ambiguous create only after Vast confirmed absence."""
    creation_error = state.get('creation_error')
    never_observed = not state.get('instance_id') and not state.get('provider_seen_at')
    if not (isinstance(creation_error, str) and creation_error and never_observed):
        return None
    return ('Rental was not created: ' + creation_error
            + '. Vast confirms no instance exists; refresh offers and select another host.')



def finalize_verified_rental(store: JobStore, identifier: str, *, now: float) -> None:
    """Finish local tracking after verified absence even if the job controller died."""
    state = store.read(identifier)
    if state['rental_state'] != 'deletion_verified':
        return
    if state.get('kind') == 'worker':
        from vast_queue import CloudQueue, worker_step
        worker_step(CloudQueue(store), identifier, now=now)
    elif state['status'] not in {'completed', 'failed', 'cancelled'}:
        collected = state.get('final_collected')
        stopped = store.read(identifier, 'control.json')['stop']
        creation_message = uncreated_rental_message(state)
        message = ('Raw results saved locally; result import needs retry' if collected else
                   creation_message if creation_message else
                   'Vast.ai instance disappeared; the last local snapshot remains available'
                   if state.get('rental_end_reason') == 'provider_instance_missing' else
                   'Rental ended; the last local snapshot remains available')
        store.update(identifier, status='cancelled' if stopped and not collected else 'failed', error=message)

def guard_step(store: JobStore, identifier: str, client: VastClient, intent: dict, registry_token: str,
               *, now: float | None = None) -> bool:
    """Reconcile one lease step; creation is never blindly retried after timeout."""
    now = time.time() if now is None else now
    directory = store.directory(identifier)
    control = store.read(identifier, "control.json")
    state = store.read(identifier)
    ending = bool(control["cleanup"] or (control["stop"] and ((state.get("kind") != "worker" and not state.get("uploaded")) or (state.get("kind") == "worker" and not state.get("active_job"))))) or now >= intent["deadline"]
    marker = directory / "attempt.json"
    if state.get('rental_state') == 'deletion_verified':
        finalize_verified_rental(store, identifier, now=now)
        return True
    observed = bool(state.get('provider_seen_at')) or state.get('rental_state') == 'active'
    if not marker.exists() and not ending and not state.get('instance_id') and not observed:
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
    try:
        # Each absence confirmation must come from a fresh, successful provider list.
        row = owned_instance(client, intent, fresh=True, expected_id=state.get('instance_id'))
    except Exception:
        store.update(identifier, absent_checks=0)
        raise
    if row:
        if state.get("instance_id") and row["id"] != state["instance_id"]:
            store.update(identifier, absent_checks=0)
            raise VastError("Instance ID does not match the rental authorization")
        store.update(identifier, instance_id=row["id"], rental_state="destroy_pending" if ending else "active",
                     provider_status=str(row.get("actual_status") or "unknown")[:50], absent_checks=0, cleanup_error=None, creation_error=None,
                     provider_seen_at=now, provider_checked_at=now,
                     **({'host_machine_id': row['machine_id']} if type(row.get('machine_id')) is int and row['machine_id'] > 0 else {}))
        if ending:
            client.request("DELETE", f"/instances/{positive_id(row['id'])}")
        return False
    # A confirmed creation may disappear before its first visible listing. Give
    # provider propagation the same grace period as an ambiguous create response.
    if ending or observed or state.get('instance_id') or marker.exists():
        # Allow a timed-out creation to settle before accepting fresh absence
        # checks. A provider error never counts as proof of absence.
        attempted_at = store.read(identifier, 'attempt.json')['attempted_at'] if marker.exists() else intent['accepted_at']
        settle_until = min(intent['deadline'], attempted_at + 120)
        if marker.exists() and (not state.get("instance_id") or (not ending and not observed)) and now < settle_until:
            store.update(identifier, cleanup_wait_until=settle_until, provider_checked_at=now,
                         cleanup_error=None)
            if not state.get('cleanup_wait_until'):
                _log(SERVICE, f"Rental {identifier}: no instance found; awaiting ambiguous-create deadline "
                     f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(settle_until))}", level='INFO')
            return False
        count = int(state.get("absent_checks", 0))
        if count and now - float(state.get('provider_checked_at') or 0) < 10:
            return False
        count += 1
        verified = count >= 2
        updates = dict(absent_checks=count, cleanup_error=None, cleanup_wait_until=None,
                       provider_checked_at=now, rental_state="deletion_verified" if verified else
                       "destroy_pending" if ending else state['rental_state'],
                       provider_status="deleted" if verified else "not_found")
        if verified and uncreated_rental_message(state):
            updates['rental_end_reason'] = 'provider_creation_failed'
            _log(SERVICE, "Rental " + identifier + ": Vast.ai confirmed no instance was created; releasing rental lock", level="WARNING")
        elif verified and not ending:
            updates['rental_end_reason'] = 'provider_instance_missing'
            _log(SERVICE, "Rental " + identifier + ": Vast.ai confirmed the instance is absent; releasing rental lock", level="WARNING")
        store.update(identifier, **updates)
        if verified:
            finalize_verified_rental(store, identifier, now=now)
        return verified
    return False


def guard_loop(store: JobStore, identifier: str) -> None:
    """Run independently of SSH/downloads until provider absence is confirmed."""
    from vast_deadline import effective_intent, reconcile_deadline
    original = validate_intent(store.read(identifier, "intent.json"), identifier)
    while True:
        try:
            intent = effective_intent(store, identifier, original)
            secrets = VastCredentialStore(store.root).secrets()
            client = VastClient(secrets["api_key"])
            state = store.read(identifier)
            if state.get('rental_state') == 'active' and time.time() < intent['deadline'] - 300:
                try:
                    row = owned_instance(client, original)
                    if row is not None:
                        reconcile_deadline(store, identifier, client, row, intent)
                except Exception as exc:
                    _log(SERVICE, 'Deadline synchronization unavailable: ' + type(exc).__name__, level='WARNING')
                intent = effective_intent(store, identifier, original)
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


def recover_cancelled_empty_results(store: JobStore) -> int:
    """Repair legacy requested stops that failed only while importing an empty result."""
    recovered = 0
    for state in store.list():
        if not (state.get('status') == 'failed'
                and state.get('final_collected') is True
                and state.get('stop_reason') == 'requested'
                and state.get('error') == 'Raw results saved locally; result import needs retry'):
            continue
        identifier = state.get('id')
        if not isinstance(identifier, str):
            continue
        final = store.directory(identifier) / 'final-results'
        try:
            finished = json.loads((final / 'finished.json').read_text())
            binaries = list((final / 'optimize_results').glob('*/all_results.bin'))
            empty_result = (len(binaries) == 1 and binaries[0].is_file()
                            and not binaries[0].is_symlink() and binaries[0].stat().st_size == 0)
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            continue
        if not (finished.get('cancelled') is True
                and empty_result):
            continue
        store.update(identifier, status='cancelled', error=None,
                     exit_code=finished.get('exit_code'), completion_reason='requested',
                     elapsed_seconds=finished.get('wall_seconds'),
                     finished_at=finished.get('finished_at'))
        recovered += 1
        _log(SERVICE, f'{identifier}: recovered requested stop with no exact results', level='INFO')
    return recovered


def finalize_collected_results(store: JobStore, identifier: str) -> dict:
    """Publish final results and assess their front without rewriting the stop cause."""
    from vast_convergence import observe
    final = store.directory(identifier) / 'final-results'
    finished = json.loads((final / 'finished.json').read_text())
    state = store.read(identifier)
    if state.get('kind') == 'calibration':
        try:
            from vast_calibration import finalize_calibration_result
            candidate = finalize_calibration_result(store, identifier)
        except (OSError, ValueError, TypeError, KeyError) as exc:
            _log(SERVICE, f'{identifier}: invalid GPU calibration evidence: {exc}', level='WARNING')
            return store.update(
                identifier, status='cancelled' if finished.get('cancelled') else 'failed',
                error='GPU calibration did not produce valid reusable evidence',
                exit_code=finished.get('exit_code'), elapsed_seconds=finished.get('wall_seconds'),
                finished_at=finished.get('finished_at'),
            )
        return store.update(
            identifier, status='completed', error=None, exit_code=finished.get('exit_code'),
            elapsed_seconds=finished.get('wall_seconds'), finished_at=finished.get('finished_at'),
            calibration_profile_candidate=candidate,
        )
    reason = state.get('stop_reason')
    historical_convergence = reason == 'convergence' or (
        reason is None and bool(state.get('convergence', {}).get('stop_requested')))
    automatic_reason = 'convergence' if historical_convergence else reason if reason == 'rental_deadline' else None
    automatic_stop = automatic_reason is not None and finished.get('cancelled') and finished.get('exit_code') in (0, -2, 130)
    status = 'cancelled' if finished.get('cancelled') else 'completed' if finished.get('exit_code') == 0 else 'failed'
    if automatic_stop:
        status = 'completed'
    binaries = list((final / 'optimize_results').glob('*/all_results.bin'))
    if status == 'cancelled' and len(binaries) == 1 and binaries[0].is_file() and binaries[0].stat().st_size == 0:
        return store.update(identifier, status='cancelled', error=None,
                            exit_code=finished.get('exit_code'), completion_reason=reason,
                            elapsed_seconds=finished.get('wall_seconds'), finished_at=finished.get('finished_at'))
    imported = import_results(store, identifier)
    store.update(identifier, **imported, exact_completed=imported.get('evaluations', state.get('exact_completed', 0)))
    observe(store, identifier, final=True)
    return store.update(identifier, status=status, error=None, exit_code=finished.get('exit_code'),
                        completion_reason=automatic_reason if automatic_stop else reason if reason != 'convergence' else None,
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
    original_intent = intent
    from vast_deadline import effective_intent
    while True:
        intent = effective_intent(store, lease_id or identifier, original_intent)
        state = store.read(identifier)
        if state.get("final_collected"):
            try:
                finalize_collected_results(store, identifier)
            except Exception:
                store.update(identifier, status="failed", error="Raw results saved locally; result import needs retry")
            if not lease_id:
                store.control(identifier, "cleanup")
            return
        lease_state = store.read(lease_id or identifier)
        if lease_state["rental_state"] == "deletion_verified":
            if state["status"] not in {"completed", "failed", "cancelled"}:
                creation_message = uncreated_rental_message(lease_state)
                store.update(identifier, status="cancelled" if store.read(identifier, "control.json")["stop"] else "failed",
                             error=(creation_message if creation_message else
                                    "Vast.ai instance disappeared; the last local snapshot remains available"
                                    if lease_state.get('rental_end_reason') == 'provider_instance_missing'
                                    else "Rental ended; the last local snapshot remains available"))
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
        if (not state.get("worker_ready") and not state.get("uploaded") and state.get("setup_started_at") is not None
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
        if state.get('worker_ready') and not state.get('uploaded'):
            if remaining <= UPLOAD_FINISH_RESERVE_SECONDS:
                store.update(identifier, status='failed', error='Input upload could not finish before the rental collection reserve; partial data retained')
                if not lease_id:
                    store.control(identifier, 'cleanup')
                return
            retry_at = state.get('upload_retry_at') or 0
            if retry_at > time.time():
                time.sleep(min(5, retry_at - time.time()))
                continue
        try:
            client = VastClient(VastCredentialStore(store.root).secrets()["api_key"])
            from vast_provisioning_log import collect_provisioning_log
            row = owned_instance(client, intent)
            if row is not None and row.get("actual_status") == "running" and state.get("setup_started_at") is None:
                state = store.update(identifier, setup_started_at=time.time())
                _log(SERVICE, f"{identifier}: instance running; verifying SSH identity and preparing worker", level="INFO")
            # During image loading the daemon log drives the progress display.
            # Once running, bootstrap owns Vast's single request_logs result
            # slot so a daemon-log request cannot replace its host-key log.
            if (row is not None and row.get("actual_status") != "running"
                    and not state.get('uploaded')):
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
                if hardware.get("revision") != intent["pb8_revision"] or guard.get("instance_id") != row["id"] or guard.get("job_id") != (lease_id or identifier) or guard.get("deadline") != intent["deadline"]:
                    raise VastError("Worker identity or independent deadline guard mismatch", 422)
                if state.get('auto_cpu_workers'):
                    from vast_transfer import allocated_cpu_workers
                    state = store.update(identifier, workers=allocated_cpu_workers(
                        hardware.get('cpu_cores'), intent.get('offer', {}).get('cpu_cores')), cpu_allocation_resolved=True)
                if not state.get('auto_cpu_workers') and (hardware.get("cpu_cores") or 0) < state["workers"]:
                    raise VastError("Actual CPU quota is below the requested worker count", 422)
                state = store.update(identifier, worker_ready=True, hardware=hardware, status="uploading", error=None)
                from vast_runtime_metrics import sample_metrics
                sample_metrics(connection, store, identifier)
                state = store.read(identifier)
                if state.get('kind') == 'calibration' and state.get('calibration_watch_preferences'):
                    minimum = state['calibration_watch_preferences'].get('min_power_watts', 0)
                    measured = (state.get('runtime_metrics') or {}).get('gpu_power_limit_watts')
                    if minimum and (measured is None or measured < minimum):
                        raise VastError('Measured GPU power limit does not meet the waiting test minimum', 422)
                collect_provisioning_log(
                    store, identifier, client, row['id'], retry_waiting=True
                )
            if control["stop"] and not state.get("uploaded"):
                store.update(identifier, status="cancelled")
                if not lease_id:
                    store.control(identifier, "cleanup")
                return
            if not state.get("uploaded"):
                if digest(directory / "input.tar.gz") != job_intent["bundle_sha256"]:
                    raise VastError("Local input bundle changed after authorization", 422)
                attempts = int(state.get("upload_attempts", 0))
                store.update(identifier, upload_attempts=attempts + 1, upload_retry_at=None)
                connection.upload(timeout=max(1, int(intent["deadline"] - time.time() - UPLOAD_FINISH_RESERVE_SECONDS)))
                store.update(identifier, uploaded=True, error=None, upload_recovery_since=None,
                             upload_retry_at=None, upload_retry_failures=0, upload_retry_bytes=0)
            progress = connection.operation("status")
            if state.get('kind') == 'calibration' and isinstance(progress.get('calibration'), dict):
                state = store.update(identifier, calibration_result=progress['calibration'])
            if not progress.get("started"):
                connection.start()
            elif not state.get('started_at'):
                store.update(identifier, started_at=optimizer_started_at(connection))
            if time.time() - last_metrics >= 15:
                from vast_runtime_metrics import sample_metrics
                sample_metrics(connection, store, identifier)
                last_metrics = time.time()
            if time.time() - last_log >= 15:
                sync_optimizer_log(connection, store, identifier)
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
            if time.time() - last_backup >= 60 and progress.get("exact_completed", 0) > 0:
                if state.get("downloaded_bytes", 0) < 1024**3:
                    connection.collect(False, timeout=max(1, min(60, int(remaining - 180))))
                    store.update(identifier, **import_results(store, identifier, partial=True))
                    observe(store, identifier)
                elif (store.read(identifier).get('convergence_config') or {}).get('convergence_enabled'):
                    from vast_convergence import observe_points
                    points = connection.convergence_snapshot()
                    observe_points(store, identifier, points, progress.get('exact_completed', 0))
                    store.update(identifier, last_convergence_snapshot_at=time.time(),
                                 convergence_snapshot_source='remote_pareto')
                last_backup = time.time()
        except Exception as exc:
            safe_error = str(exc) if isinstance(exc, VastError) else "Worker operation failed; reconnecting"
            _log(SERVICE, safe_error, level="WARNING",
                 meta=None if isinstance(exc, VastError) else {'traceback': traceback.format_exc()})
            current = store.read(identifier)
            if current.get('worker_ready') and not current.get('uploaded'):
                stopped = any(store.read(owner, 'control.json').get(key)
                              for owner in {identifier, lease_id or identifier} for key in ('stop', 'cleanup'))
                if stopped:
                    store.update(identifier, status='cancelled', error=None, upload_retry_at=None)
                    if not lease_id:
                        store.control(identifier, 'cleanup')
                    return
                try:
                    schedule_upload_retry(store, identifier, exc, intent['deadline'])
                except VastError as fatal:
                    exc = fatal
                    safe_error = str(fatal)
                    _log(SERVICE, safe_error, level='WARNING')
                else:
                    continue
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
        store.update(identifier, **{'supervisor_' + mode: {
            'pid': os.getpid(), 'created_at': psutil.Process().create_time(),
            'code_serial': RUNNER_CODE_SERIAL}})
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
