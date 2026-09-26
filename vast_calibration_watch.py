"""Durable GPU calibration offer watch with explicit paid consent."""

from __future__ import annotations

import time

from file_lock import advisory_file_lock
from vast_credentials import VastCredentialStore
from vast_hosts import offer_priority, search_host_offers
from vast_jobs import job_id
from vast_provider import VastClient, VastError, gpu_name_matches, number
from vast_queue import CloudQueue, blocked_machine_ids, offer_host_allowed

SERVICE = 'VastPool'
WATCH_INTERVAL_SECONDS = 30


def validated_watch(queue: CloudQueue) -> dict | None:
    """Reject damaged persisted consent before any provider request or rental."""
    watch = queue.read().get('calibration_watch')
    if watch is None:
        return None
    if not isinstance(watch, dict) or not isinstance(watch.get('preferences'), dict):
        raise VastError('Waiting performance-test authorization is invalid', 422)
    try:
        job_id(watch['id'])
        job_id(watch['job_id'])
        preferences = watch['preferences']
        name = preferences['gpu_name']
        if (not isinstance(name, str) or not name.strip() or len(name) > 80
                or any(ord(char) < 32 or ord(char) == 127 for char in name)
                or type(preferences['verified_only']) is not bool
                or type(preferences['disk_gb']) is not int):
            raise ValueError('identity')
        limits = {
            'max_price': (0, 100), 'min_vram': (0, 256),
            'min_ram': (0, 4096), 'min_cpu': (0, 512),
            'min_tflops': (0, 100_000), 'min_power_watts': (0, 1000),
            'min_reliability_pct': (0, 100), 'disk_gb': (40, 2000),
        }
        if any(number(preferences[key]) is None or not lower <= preferences[key] <= upper
               for key, (lower, upper) in limits.items()):
            raise ValueError('limits')
        if preferences['max_price'] <= 0:
            raise ValueError('price')
        hours, budget = number(watch['hours']), number(watch['budget'])
        accepted = number(watch['accepted_at'])
        if (hours is None or budget is None or accepted is None
                or not .75 <= hours <= 24 or not .1 <= budget <= 100
                or hours * preferences['max_price'] > budget + 1e-6
                or type(watch['credential_generation']) is not int
                or watch['credential_generation'] < 0):
            raise ValueError('consent')
    except (KeyError, TypeError, ValueError, VastError):
        raise VastError('Waiting performance-test authorization is invalid', 422) from None
    return watch


def offer_matches(watch: dict, offer: dict, blocked: list[int]) -> bool:
    """Recheck every frozen filter on the exact offer about to be rented."""
    preferences = watch['preferences']
    return bool(
        offer_host_allowed(offer, blocked)
        and gpu_name_matches(offer.get('gpu_name') or '', preferences['gpu_name'])
        and offer.get('num_gpus') == 1
        and type(offer.get('machine_id')) is int
        and (offer.get('cuda_max_good') or 0) >= 13
        and 0 < (offer.get('price_hour_usd') or 0) <= preferences['max_price']
        and (offer.get('vram_gb') or 0) >= preferences['min_vram']
        and (offer.get('ram_gb') or 0) >= preferences['min_ram']
        and (offer.get('cpu_cores') or 0) >= preferences['min_cpu']
        and (offer.get('tflops') or 0) >= preferences['min_tflops']
        and (offer.get('gpu_max_power_watts') or 0) >= preferences['min_power_watts']
        and (offer.get('reliability') or 0) * 100 >= preferences['min_reliability_pct']
        and (offer.get('disk_gb') or 0) >= preferences['disk_gb']
        and (not preferences['verified_only'] or offer.get('verified') is True)
        and (offer.get('duration_seconds') is None
             or offer['duration_seconds'] >= watch['hours'] * 3600)
    )


def cancel_watch(queue: CloudQueue, watch_id: str, reason: str) -> bool:
    """Revoke a watch, including any unconfirmed rental attempt."""
    with advisory_file_lock(queue.root / '.queue-lock'):
        current = queue.read().get('calibration_watch')
        if not isinstance(current, dict) or current.get('id') != watch_id:
            return False
        queue.update(calibration_watch=None)
        for worker in queue.workers():
            if (worker.get('calibration_job_id') == current['job_id']
                    and not worker.get('instance_id') and not worker.get('provider_seen_at')):
                queue.store.control(worker['id'], 'cleanup')
        identifier = job_id(current['job_id'])
        row = queue.store.read(identifier)
        if not row.get('lease_id') and row.get('status') in {'preparing', 'ready'}:
            queue.store.control(identifier, 'stop')
            queue.store.update(identifier, status='cancelled', error=reason)
        return True


def _refresh_prepared_input(queue: CloudQueue, watch: dict, now: float) -> None:
    """Replace stale or failed input without ending an indefinite watch."""
    from api.optimize_v8 import _results_root
    from pb8_config import get_pb8_template_config
    from vast_calibration import canonical_calibration_config
    import hashlib
    import json
    from pathlib import Path

    store = queue.store
    previous = store.read(watch['job_id'])
    try:
        # Do not create repeated failed jobs while PBGui's market cache is stale.
        from vast_market_cache import _load_local_markets
        _load_local_markets(['binance'])
        canonical = canonical_calibration_config(get_pb8_template_config())
        source_sha256 = hashlib.sha256(
            json.dumps(canonical, sort_keys=True, separators=(',', ':')).encode()
        ).hexdigest()
        replacement = store.create_preparation('PBGui GPU calibration v1', 10_000_000, 4, False)
        store.update(replacement['id'], kind='calibration',
                     calibration_plan=previous['calibration_plan'],
                     calibration_workload_version=previous['calibration_workload_version'],
                     calibration_watch_preferences=watch['preferences'],
                     estimated_coin_candles=previous.get('estimated_coin_candles'))
        root = Path(__file__).resolve().parent
        store.prepare('PBGui GPU calibration v1', canonical, source_sha256,
                      root / 'data/ohlcv', root / 'data/coindata', _results_root(),
                      10_000_000, 4, False, identifier=replacement['id'])
    except Exception as exc:
        with advisory_file_lock(queue.root / '.queue-lock'):
            current = queue.read().get('calibration_watch')
            if isinstance(current, dict) and current.get('id') == watch['id']:
                queue.update(calibration_watch=dict(current, last_checked_at=now,
                    next_check_at=now + 300, last_error=str(exc) or 'Input refresh failed'))
        return
    with advisory_file_lock(queue.root / '.queue-lock'):
        current = queue.read().get('calibration_watch')
        if isinstance(current, dict) and current.get('id') == watch['id']:
            queue.update(calibration_watch=dict(current, job_id=replacement['id'],
                         last_checked_at=now, next_check_at=now, last_error=None))
            if previous.get('status') == 'ready':
                store.update(previous['id'], status='cancelled',
                             error='Replaced by a fresh waiting-test input')
        else:
            store.update(replacement['id'], status='cancelled',
                         error='Waiting test was cancelled during input refresh')


def watch_step(queue: CloudQueue) -> dict | None:
    """Retry verified-uncreated offers until one rental succeeds or cancellation."""
    watch = validated_watch(queue)
    if watch is None:
        return None
    now = time.time()
    job = queue.store.read(watch['job_id'])
    if job.get('status') in {'cancelled', 'completed'}:
        cancel_watch(queue, watch['id'], 'Waiting performance test ended without a rental')
        return None
    # A returned contract, or any later provider sighting, consumes the paid
    # authorization even if the rental was already cleaned up between polls.
    previous = (worker for worker in queue.workers(active_only=False)
                if worker.get('calibration_job_id') == watch['job_id'])
    if any(worker.get('instance_id') or worker.get('provider_seen_at') for worker in previous):
        with advisory_file_lock(queue.root / '.queue-lock'):
            current = queue.read().get('calibration_watch')
            if isinstance(current, dict) and current.get('id') == watch['id']:
                queue.update(calibration_watch=None)
        return None
    # An unresolved create may still have succeeded at Vast. Its guard must
    # verify absence before any other paid request is allowed.
    if queue.workers() or job.get('status') not in {'ready', 'failed'}:
        return None
    if now < (number(watch.get('next_check_at')) or 0):
        return None
    credentials = VastCredentialStore(queue.root)
    if credentials.metadata()['generation'] != watch['credential_generation']:
        cancel_watch(queue, watch['id'], 'Vast credentials changed; waiting test authorization revoked')
        return None
    preferences = watch['preferences']
    state = queue.read()
    blocked = blocked_machine_ids(state)
    client = VastClient(credentials.secrets()['api_key'])
    rows = search_host_offers(client, state,
        max_price=preferences['max_price'], min_vram=preferences['min_vram'],
        min_ram=preferences['min_ram'], min_cpu=preferences['min_cpu'],
        min_tflops=preferences['min_tflops'], min_power_watts=preferences['min_power_watts'],
        min_reliability_pct=preferences['min_reliability_pct'],
        disk_gb=preferences['disk_gb'], verified_only=preferences['verified_only'],
        gpu_name=preferences['gpu_name'], min_cuda=13,
        min_duration=watch['hours'] * 3600, excluded_machine_ids=blocked, max_offers=500)
    matches = [row for row in rows if offer_matches(watch, row, blocked)]
    with advisory_file_lock(queue.root / '.queue-lock'):
        current = queue.read().get('calibration_watch')
        if not isinstance(current, dict) or current.get('id') != watch['id']:
            return None
        if not matches:
            queue.update(calibration_watch=dict(current, last_checked_at=now,
                         next_check_at=now + WATCH_INTERVAL_SECONDS, last_error=None))
            return None
    if job.get('status') == 'failed':
        _refresh_prepared_input(queue, watch, now)
        return None
    try:
        from vast_queue import preflight_local_metadata
        preflight_local_metadata(queue.store, [job])
    except VastError:
        _refresh_prepared_input(queue, watch, now)
        return None
    offer = min(matches, key=lambda row: offer_priority(row, state))
    try:
        refreshed = client.offers(
            max_price=preferences['max_price'], min_vram=preferences['min_vram'],
            min_ram=preferences['min_ram'], min_cpu=preferences['min_cpu'],
            min_tflops=preferences['min_tflops'], disk_gb=preferences['disk_gb'],
            min_power_watts=preferences['min_power_watts'],
            min_reliability_pct=preferences['min_reliability_pct'],
            verified_only=preferences['verified_only'], offer_id=offer['id'],
            min_cuda=13, min_duration=watch['hours'] * 3600,
            excluded_machine_ids=blocked, max_offers=1,
        )
        current_offer = next((row for row in refreshed
            if row.get('id') == offer['id']
            and row.get('machine_id') == offer['machine_id']
            and row.get('gpu_name') == offer['gpu_name']
            and abs(float(row.get('vram_gb') or 0) - float(offer.get('vram_gb') or 0)) < .01
            and offer_matches(watch, row, blocked)), None)
        if current_offer is None:
            raise VastError('Matching offer changed or disappeared before rental; waiting for another', 409)
        return queue.start(current_offer, watch['hours'], watch['budget'], 0,
                           calibration_id=watch['job_id'], calibration_watch_id=watch['id'])
    except VastError as exc:
        with advisory_file_lock(queue.root / '.queue-lock'):
            current = queue.read().get('calibration_watch')
            if isinstance(current, dict) and current.get('id') == watch['id']:
                if not queue.workers():
                    queue.update(calibration_watch=dict(current, last_checked_at=now,
                                 next_check_at=now + WATCH_INTERVAL_SECONDS, last_error=str(exc)))
        return None
