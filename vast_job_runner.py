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
from file_lock import advisory_file_lock
from vast_credentials import VastCredentialStore
from vast_jobs import SUPPORTED_RENTAL_IMAGES, SUPPORTED_RENTAL_IMAGE_REVISIONS, PROJECT, JobStore, digest, job_id, write_json
from vast_provider import VastRateLimit, VastClient, VastError, positive_id
from vast_transfer import WorkerConnection, import_results
from vast_snapshot import snapshot, bounded_lock

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
    from vast_throughput import LOG_TAIL_BYTES
    try:
        raw_log = connection.command('if test -f ' + connection.remote_root + '/output/optimizer.log; then tail -c '
                                     + str(LOG_TAIL_BYTES) + ' ' + connection.remote_root + '/output/optimizer.log; fi',
                                     max_output=LOG_TAIL_BYTES * 2)
        raw_log = raw_log[-LOG_TAIL_BYTES:]
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
    with advisory_file_lock(store.directory(identifier) / '.state-lock'):
        state = store.read(identifier)
        if state['rental_state'] != 'deletion_verified':
            return
        worker = state.get('kind') == 'worker'
        if not worker and state['status'] not in {'completed', 'failed', 'cancelled'}:
            collected = state.get('final_collected')
            stopped = store.read(identifier, 'control.json')['stop']
            creation_message = uncreated_rental_message(state)
            message = ('Raw results saved locally; result import needs retry' if collected else
                       creation_message if creation_message else
                       'Vast.ai instance disappeared; the last local snapshot remains available'
                       if state.get('rental_end_reason') == 'provider_instance_missing' else
                       'Rental ended; the last local snapshot remains available')
            store.update(identifier, status='cancelled' if stopped and not collected else 'failed', error=message)
    if worker:
        from vast_queue import CloudQueue, worker_step
        worker_step(CloudQueue(store), identifier, now=now)

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
        if intent.get('gpu_replacement'):
            from pb8_loop_store import LoopStore
            from pb8_loop_gpu_replacement import validate_rental_authorization
            from vast_queue import CloudQueue
            loops = LoopStore(PROJECT / 'data/loop_optimizer')
            queue = CloudQueue(store)
            def authorize():
                """Retain a visible reason while the same unsent reservation waits for permission."""
                current = loops.read(state['loop_owner'], state['loop_id'])
                try:
                    validate_rental_authorization(queue, current, intent['gpu_replacement'])
                except VastError as exc:
                    store.update(identifier, authorization_wait_reason=str(exc))
                    raise
                return current
            # Dataset verification must allow user controls and storage pauses
            # to commit. It proves inputs, never authorizes a later paid effect.
            record = authorize()
            from pb8_loop_backend import LoopBackend
            operation_id = intent['gpu_replacement']['operation_id']
            operation = record['gpu_replacements'][operation_id]
            successor = next(job for job in record['jobs'] if job['operation'] == operation['successor'])
            try:
                LoopBackend(PROJECT, loops).verify_replacement_input(record, successor)
            except (ValueError, VastError) as exc:
                store.update(identifier, authorization_wait_reason=str(exc))
                return False
            with advisory_file_lock(loops.path(state['loop_owner'], state['loop_id'])):
                # Revalidate controls and the exact verified input after the
                # unlocked scan, then serialize the durable Create claim with
                # capacity/preference/host-block changes. No provider wait may
                # retain either transaction lock.
                with advisory_file_lock(queue.root / '.queue-lock'):
                    current = authorize()
                    current_operation = current['gpu_replacements'][operation_id]
                    current_successor = next(job for job in current['jobs']
                        if job['operation'] == current_operation['successor'])
                    if (current['fingerprint'] != record['fingerprint']
                            or current['initial_config'] != record['initial_config']
                            or current_operation['input_digest'] != operation['input_digest']
                            or current_successor != successor):
                        reason = 'Preserved GPU replacement input changed during verification; review is required'
                        store.update(identifier, authorization_wait_reason=reason)
                        raise VastError(reason, 409)
                    fresh_control = store.read(identifier, 'control.json')
                    if fresh_control.get('stop') or fresh_control.get('cleanup'):
                        return False
                    fresh_state = store.read(identifier)
                    if (marker.exists() or fresh_state.get('instance_id')
                            or fresh_state.get('rental_state') != 'creation_pending'):
                        return False  # Reconcile an already claimed effect on the next tick.
                    store.update(identifier, authorization_wait_reason=None)
                    # Slow frozen-input verification must not consume the
                    # provider's propagation grace before Create is even sent.
                    now = time.time()
                    write_json(marker, {"attempted_at": now})
            # The request is now durably claimed. Controls can commit while its
            # response is pending; the marker keeps ambiguous effects on the
            # existing reconciliation/cleanup path instead of renting twice.
            try:
                response = client.request('PUT', f"/asks/{intent['offer']['id']}/", rental_payload(intent, registry_token))
            except VastRateLimit as exc:
                if not exc.request_sent:
                    marker.unlink()
                else:
                    store.update(identifier, creation_error=str(exc))
                raise
            except VastError as exc:
                store.update(identifier, creation_error=str(exc))
                raise
            store.update(identifier, instance_id=positive_id(response.get('new_contract')))
            # The normal fresh provider observation below owns confirmation.
            state = store.read(identifier)
        else:
            return _create_and_observe(store, identifier, client, intent, registry_token, now)
    return _observe_rental(store, identifier, client, intent, now, ending, observed, marker, state)


def _create_and_observe(store, identifier, client, intent, registry_token, now):
    """Preserve the ordinary rental Create and ambiguity handling."""
    marker = store.directory(identifier) / 'attempt.json'
    ending, observed = False, False
    if not marker.exists():
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
    else:
        state = store.read(identifier)
    return _observe_rental(store, identifier, client, intent, now, ending, observed, marker, state)


def _observe_rental(store, identifier, client, intent, now, ending, observed, marker, state):
    """Only fresh provider absence can release a rental authorization."""
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
        if ending and now - float(state.get('destroy_requested_at') or 0) >= 60:
            client.request("DELETE", f"/instances/{positive_id(row['id'])}/")
            store.update(identifier, destroy_requested_at=now)
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
            if original.get('gpu_replacement') and secrets['generation'] != original['credential_generation']:
                raise VastError('Replacement rental credentials changed; review the provider account before continuing', 409)
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
    rows, _unreadable = store.list_with_errors()
    for state in rows:
        if not (state.get('status') == 'failed'
                and state.get('final_collected') is True
                and state.get('stop_reason') == 'requested'
                and state.get('error') == 'Raw results saved locally; result import needs retry'):
            continue
        identifier = state.get('id')
        if not isinstance(identifier, str):
            continue
        with advisory_file_lock(store.directory(identifier) / '.finalize-lock'):
            state = store.read(identifier)
            if not (state.get('status') == 'failed'
                    and state.get('final_collected') is True
                    and state.get('stop_reason') == 'requested'
                    and state.get('error') == 'Raw results saved locally; result import needs retry'):
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


def _finalization_complete(store: JobStore, identifier: str, state: dict, finished: dict) -> bool:
    """Recognize a fully published outcome, never just a result path or terminal state."""
    if not isinstance(finished, dict) or state.get('error') == 'Raw results saved locally; result import needs retry':
        return False
    if state.get('kind') == 'calibration':
        return (state.get('status') == 'completed' and bool(state.get('calibration_profile_candidate'))
                and state.get('error') is None and all(state.get(key) == finished.get(source) for key, source in (
                    ('exit_code', 'exit_code'), ('elapsed_seconds', 'wall_seconds'), ('finished_at', 'finished_at'))))
    reason = state.get('stop_reason')
    historical = reason == 'convergence' or (reason is None and bool(state.get('convergence', {}).get('stop_requested')))
    automatic = 'convergence' if historical else reason if reason == 'rental_deadline' else None
    automatic_stop = automatic and finished.get('cancelled') and finished.get('exit_code') in (0, -2, 130)
    status = 'completed' if automatic_stop else ('cancelled' if finished.get('cancelled') else
                                               'completed' if finished.get('exit_code') == 0 else 'failed')
    completion = automatic if automatic_stop else reason if reason != 'convergence' else None
    if any(state.get(key) != value for key, value in {
        'status': status, 'exit_code': finished.get('exit_code'), 'completion_reason': completion,
        'elapsed_seconds': finished.get('wall_seconds'), 'finished_at': finished.get('finished_at'),
    }.items()):
        return False
    if status != 'failed' and state.get('error') is not None:
        return False
    if status == 'failed' and not str(state.get('error', '')).startswith(
            f"PB8 optimizer exited with code {finished.get('exit_code')}"):
        return False
    binaries = list((store.directory(identifier) / 'final-results/optimize_results').glob('*/all_results.bin'))
    if (status == 'cancelled' and len(binaries) == 1 and binaries[0].is_file()
            and not binaries[0].is_symlink() and binaries[0].stat().st_size == 0):
        return True
    if status == 'failed' and not any(path.is_file() and not path.is_symlink() and path.stat().st_size > 0 for path in binaries):
        return True
    if not state.get('result_path') or state.get('result_partial') is not False:
        return False
    evaluations = state.get('evaluations')
    if type(evaluations) is not int or evaluations <= 0 or state.get('exact_completed') != evaluations:
        return False
    if state.get('convergence_config', {}).get('convergence_enabled'):
        assessment = state.get('convergence', {}).get('final')
        if (not isinstance(assessment, dict) or not isinstance(assessment.get('phase'), str)
                or type(assessment.get('threshold_reached')) is not bool
                or not ((type(assessment.get('checked_exact')) is int and assessment['checked_exact'] == evaluations)
                        or ('checked_exact' not in assessment and assessment.get('phase') == 'waiting'
                            and isinstance(assessment.get('reason'), str)))):
            return False
    try:
        sources = list((store.directory(identifier) / 'final-results/optimize_results').glob('*'))
        root = Path(store.read(identifier, 'intent.json')['results_root'])
        if len(sources) != 1 or not root.is_absolute() or root.is_symlink() or root.name != 'optimize_results':
            return False
        target = root / (sources[0].name + '_vast_' + identifier[:12])
        if str(target) != state['result_path'] or target.is_symlink():
            return False
        provenance_path = target / '.pbgui_vast.json'
        if provenance_path.is_symlink() or provenance_path.stat().st_size > 65536:
            return False
        provenance = json.loads(provenance_path.read_text())
        binary = target / 'all_results.bin'
        return (provenance.get('job_id') == identifier and provenance.get('partial') is False
                and provenance.get('evaluations') == evaluations and binary.is_file()
                and not binary.is_symlink() and binary.stat().st_size > 0)
    except (OSError, ValueError, TypeError, KeyError):
        return False


def _persist_finalization_error(store: JobStore, identifier: str, *, preserve_execution=False) -> None:
    """Persist technical retry state under Finalize, preserving an intervening complete outcome."""
    try:
        state = store.read(identifier)
        finished_path = store.directory(identifier) / 'final-results/finished.json'
        try:
            finished = json.loads(finished_path.read_text()) if finished_path.exists() else {}
        except (OSError, ValueError, TypeError):
            finished = {}
        try:
            complete = _finalization_complete(store, identifier, state, finished)
        except Exception as exc:
            _log(SERVICE, 'Could not verify final result completion: ' + type(exc).__name__, level='WARNING')
            complete = False
        if complete:
            return
        if ((preserve_execution or state.get('stop_finalization_pending') is True)
                and state.get('final_collected') is True
                and state.get('kind') != 'calibration' and isinstance(finished, dict)
                and type(finished.get('exit_code')) is int
                and type(finished.get('cancelled', False)) is bool):
            store.update(identifier, **_optimizer_execution_outcome(store, identifier, state, finished),
                         collection_incomplete=True, stop_finalization_pending=True,
                         collection_error='Final collection could not finish; available verified snapshots and logs are retained')
            return
        store.update(identifier, status='failed', error='Raw results saved locally; result import needs retry')
    except Exception as exc:
        _log(SERVICE, 'Could not persist result import retry state: ' + type(exc).__name__, level='ERROR')


def finalize_collected_results(store: JobStore, identifier: str, *, check=None, preserve_execution=False) -> dict:
    """Serialize import, assessment and final status across API, runner and stop callers."""
    with bounded_lock(store.directory(identifier) / '.finalize-lock', check=check or (lambda: None), check_on_acquire=False), bounded_lock(store.directory(identifier) / '.snapshot-lock', check=check or (lambda: None), check_on_acquire=False):
        try:
            return _finalize_collected_results_locked(store, identifier, check=check)
        except Exception as exc:
            _log(SERVICE, 'Final result publication failed: ' + type(exc).__name__, level='ERROR')
            _persist_finalization_error(store, identifier, preserve_execution=preserve_execution)
            raise


def _optimizer_execution_outcome(store, identifier, state, finished):
    """Derive execution evidence independently of local result publication success."""
    reason = state.get('stop_reason')
    historical = reason == 'convergence' or (
        reason is None and bool(state.get('convergence', {}).get('stop_requested')))
    automatic = 'convergence' if historical else reason if reason == 'rental_deadline' else None
    automatic_stop = automatic is not None and finished.get('cancelled') and finished.get('exit_code') in (0, -2, 130)
    status = 'completed' if automatic_stop else ('cancelled' if finished.get('cancelled') else
                                               'completed' if finished.get('exit_code') == 0 else 'failed')
    error = None
    if status == 'failed':
        error = f"PB8 optimizer exited with code {finished.get('exit_code')}; see collected optimizer.log"
        log_path = store.directory(identifier) / 'final-results/optimizer.log'
        excerpt = b''
        try:
            if log_path.is_file() and not log_path.is_symlink():
                with log_path.open('rb') as handle:
                    excerpt = handle.read(65536)
        except OSError as exc:
            # The verified exit code is authoritative; optional diagnostics
            # must not prevent either finalization or its error persistence.
            _log(SERVICE, 'Optimizer diagnostic unavailable: ' + type(exc).__name__, level='WARNING')
            error = f"PB8 optimizer exited with code {finished.get('exit_code')}; optimizer.log could not be read"
        if b'suite mode does not support asymmetric live.approved_coins' in excerpt:
            error = (f"PB8 optimizer exited with code {finished.get('exit_code')}: Suite requires identical "
                     'live.approved_coins.long and .short lists')
        elif b'constraint agreement fell below safety threshold' in excerpt:
            error = (f"PB8 optimizer exited with code {finished.get('exit_code')}: "
                     'GPU proxy/exact constraint agreement failed the Passivbot safety check')
            if b'backtest_completion_ratio_min:' in excerpt and b'bound=1.0' in excerpt:
                error += '; strict completion limit 1.0 disagreed between GPU proxy and exact CPU results'
    return dict(status=status, error=error, exit_code=finished.get('exit_code'),
                completion_reason=automatic if automatic_stop else reason if reason != 'convergence' else None,
                elapsed_seconds=finished.get('wall_seconds'), finished_at=finished.get('finished_at'))


def _finalize_collected_results_locked(store: JobStore, identifier: str, *, check=None) -> dict:
    """Publish final results and assess their front without rewriting the stop cause."""
    from vast_convergence import observe
    final = store.directory(identifier) / 'final-results'
    finished = json.loads((final / 'finished.json').read_text())
    state = store.read(identifier)
    if _finalization_complete(store, identifier, state, finished):
        if state.get('collection_incomplete') or state.get('collection_error') or state.get('stop_finalization_pending'):
            return store.update(identifier, collection_incomplete=False, collection_error=None, stop_finalization_pending=False)
        return state
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
    outcome = _optimizer_execution_outcome(store, identifier, state, finished)
    status = outcome['status']
    binaries = list((final / 'optimize_results').glob('*/all_results.bin'))
    if status == 'failed':
        if not any(path.is_file() and not path.is_symlink() and path.stat().st_size > 0 for path in binaries):
            return store.update(identifier, **outcome, collection_incomplete=False, collection_error=None, stop_finalization_pending=False)
    if status == 'cancelled' and len(binaries) == 1 and binaries[0].is_file() and binaries[0].stat().st_size == 0:
        return store.update(identifier, **outcome, collection_incomplete=False, collection_error=None, stop_finalization_pending=False)
    imported = import_results(store, identifier, **({'check': check} if check is not None else {}))
    store.update(identifier, **imported, exact_completed=imported.get('evaluations', state.get('exact_completed', 0)))
    observe(store, identifier, final=True)
    return store.update(identifier, **outcome, collection_incomplete=False, collection_error=None, stop_finalization_pending=False)


def stop_and_collect(connection, store, identifier, intent, *, lease_id=None):
    """Bound Stop RPC, process wait and final collection across supervisor restarts."""
    with advisory_file_lock(store.directory(identifier) / '.state-lock'):
        state = store.read(identifier)
        if state.get('stop_results_published'):
            return True
        send_stop = not state.get('stop_rpc_at')
        if send_stop:
            state = store.update(identifier, stop_rpc_at=time.time(),
                                 status=state['status'] if state['status'] in {'completed', 'failed', 'cancelled'} else 'collecting',
                                 stop_reason=state.get('stop_reason') if state['status'] in {'completed', 'failed', 'cancelled'} else state.get('stop_reason') or 'requested')
    if send_stop:
        try:
            connection.operation('stop', timeout=max(.01, min(30, intent['deadline'] - time.time() - 15)))
        except Exception as exc:
            _log(SERVICE, 'Stop RPC incomplete; retaining bounded collection: ' + type(exc).__name__, level='WARNING')
    wait_until = min(state['stop_rpc_at'] + 60, intent['deadline'] - 15)
    progress = {}
    if time.time() < wait_until and not state.get('final_collection_at'):
        try:
            progress = connection.operation('status', timeout=max(.01, min(30, wait_until - time.time())))
        except Exception as exc:
            _log(SERVICE, 'Stop status unavailable: ' + type(exc).__name__, level='WARNING')
        if not progress.get('finished') and time.time() < wait_until:
            return False
    if not state.get('final_collection_at'):
        state = store.update(identifier, final_collection_at=time.time())
    deadline = min(state['final_collection_at'] + 180, intent['deadline'] - 15)
    def remaining():
        """Download and local publication share the persisted collection allowance."""
        value = deadline - time.time()
        if value <= 0:
            raise VastError('Final collection deadline reached; retaining the last verified snapshot')
        return value
    finalized = False
    collection_failed = False
    try:
        # Keep the persisted Stop allowance; lock waits do not renew it.
        with snapshot(store, identifier, check=remaining):
            current = store.read(identifier)
            final = current.get('final_collected') or bool(progress.get('finished'))
            if not current.get('stop_snapshot_collected') and not current.get('final_collected'):
                connection.collect(final, timeout=remaining())
                store.update(identifier, stop_snapshot_collected=True)
            if not final:
                sources = store.directory(identifier) / 'partial-results/optimize_results'
                if sources.is_dir():
                    imported = import_results(store, identifier, partial=True, check=remaining)
                    store.update(identifier, **imported)
                store.update(identifier, collection_incomplete=True, collection_error=None,
                             stop_results_published=True)
        # Never acquire Finalize while holding Snapshot.
        if final:
            finalize_collected_results(store, identifier, check=remaining, preserve_execution=True)
            finalized = True
            store.update(identifier, collection_incomplete=False, collection_error=None,
                         stop_results_published=True)
    except Exception as exc:
        _log(SERVICE, 'Final collection incomplete: ' + type(exc).__name__, level='WARNING')
        collection_failed = True
    if not finalized:
        # Match every finalizer's Finalize-before-State order, then re-read
        # collection and completion so a concurrent successful recovery wins.
        try:
            with bounded_lock(store.directory(identifier) / '.finalize-lock', blocking=False), bounded_lock(store.directory(identifier) / '.snapshot-lock', blocking=False):
                with bounded_lock(store.directory(identifier) / '.state-lock', blocking=False):
                    current = store.read(identifier)
                    collected = bool(current.get('final_collected'))
                    if not collected:
                        changes = {} if current['status'] in {'failed', 'completed', 'cancelled'} else {'status': 'cancelled'}
                        if collection_failed:
                            changes.update(collection_incomplete=True,
                                collection_error='Final collection could not finish; available verified snapshots and logs are retained')
                        if changes:
                            store.update(identifier, **changes)
                if collected:
                    _persist_finalization_error(store, identifier, preserve_execution=True)
        except VastError as exc:
            if 'Collection is busy' not in str(exc):
                raise
            # Another publisher owns the outcome. Retry from the persisted
            # Stop markers instead of waiting beyond the Stop deadline.
            return False
    if not lease_id:
        store.control(identifier, 'cleanup')
    return True


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
            except Exception as exc:
                # The finalizer owns error persistence under its transaction lock.
                _log(SERVICE, 'Final result import requires retry: ' + type(exc).__name__, level='WARNING')
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
                connection.verify_upload_input(job_intent['bundle_sha256'], timeout=max(1, int(intent['deadline'] - time.time() - UPLOAD_FINISH_RESERVE_SECONDS)))
                attempts = int(state.get("upload_attempts", 0))
                store.update(identifier, upload_attempts=attempts + 1, upload_retry_at=None)
                connection.upload(timeout=max(1, int(intent["deadline"] - time.time() - UPLOAD_FINISH_RESERVE_SECONDS)))
                store.update(identifier, uploaded=True, error=None, upload_recovery_since=None,
                             upload_retry_at=None, upload_retry_failures=0, upload_retry_bytes=0)
            control = store.read(identifier, 'control.json')
            if lease_id and store.read(lease_id, 'control.json')['stop']:
                store.control(identifier, 'stop')
                control['stop'] = True
            if control['stop']:
                if stop_and_collect(connection, store, identifier, intent, lease_id=lease_id):
                    return
                time.sleep(1)
                continue
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
                def final_remaining():
                    """Preserve the rental reserve even after waiting for Snapshot."""
                    current_intent = effective_intent(store, lease_id or identifier, original_intent)
                    seconds = current_intent['deadline'] - time.time() - 15
                    if seconds <= 0:
                        raise VastError('Rental collection deadline reached; last verified results retained')
                    return min(180, seconds)
                with snapshot(store, identifier, check=final_remaining):
                    if not store.read(identifier).get('final_collected'):
                        connection.collect(True, timeout=final_remaining())
                continue
            if time.time() - last_backup >= 60 and progress.get("exact_completed", 0) > 0:
                if state.get("downloaded_bytes", 0) < 1024**3:
                    def partial_remaining():
                        """Recompute the effective reserve after every lock wait."""
                        current_intent = effective_intent(store, lease_id or identifier, original_intent)
                        seconds = current_intent['deadline'] - time.time() - 180
                        if seconds <= 0:
                            raise VastError('Partial backup reserve reached; final collection takes priority')
                        return min(60, seconds)
                    with snapshot(store, identifier, check=partial_remaining):
                        current = store.read(identifier)
                        if not current.get('final_collected') and current.get('downloaded_bytes', 0) < 1024**3:
                            connection.collect(False, timeout=partial_remaining())
                            store.update(identifier, **import_results(store, identifier, partial=True, check=partial_remaining))
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
            'code_serial': RUNNER_CODE_SERIAL, **({'snapshot_transaction': 1} if mode == 'run' else {})}})
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
