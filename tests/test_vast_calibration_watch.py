"""Offline waiting GPU calibration authorization contracts."""

from __future__ import annotations

from types import SimpleNamespace
import time

import pytest

import vast_calibration_watch as watching
from vast_jobs import JobStore
from vast_provider import VastError
from vast_queue import CloudQueue


@pytest.fixture
def armed(tmp_path, monkeypatch):
    """Create a frozen watch and ready job without provider or runtime access."""
    queue = CloudQueue(JobStore(tmp_path / 'vast'))
    preparation = queue.store.create_preparation('PBGui GPU calibration v1', 10_000_000, 4, False)
    queue.store.update(preparation['id'], kind='calibration', status='ready',
                       calibration_plan={'protocol': 3}, calibration_workload_version='v2')
    preferences = {
        'gpu_name': 'RTX 3090', 'max_price': .2, 'min_vram': 24, 'min_ram': 16,
        'min_cpu': 16, 'min_tflops': 0, 'min_power_watts': 350,
        'min_reliability_pct': 95, 'disk_gb': 40, 'verified_only': True,
    }
    watch = {
        'id': 'a' * 32, 'job_id': preparation['id'], 'preferences': preferences,
        'hours': 1, 'budget': 5, 'accepted_at': time.time(),
        'credential_generation': 1, 'next_check_at': 0,
    }
    queue.update(calibration_watch=watch)
    monkeypatch.setattr(watching, 'VastCredentialStore', lambda root: SimpleNamespace(
        metadata=lambda: {'generation': 1}, secrets=lambda: {'api_key': 'fake'}))
    monkeypatch.setattr(watching, 'VastClient', lambda key: SimpleNamespace(
        offers=lambda **kwargs: [compatible_offer()]))
    monkeypatch.setattr('vast_queue.preflight_local_metadata', lambda *args: None)
    return queue, watch


def compatible_offer(**changes):
    """Return an offer that satisfies every frozen watch requirement."""
    return dict(id=42, machine_id=43, gpu_name='RTX 3090', num_gpus=1,
                cuda_max_good=13, price_hour_usd=.19, vram_gb=24, ram_gb=32,
                cpu_cores=24, tflops=35, gpu_max_power_watts=350, reliability=.99,
                disk_gb=40, verified=True, duration_seconds=7200, **changes)


@pytest.mark.parametrize('field,value', [
    ('gpu_max_power_watts', 300), ('reliability', .90), ('price_hour_usd', .21),
    ('gpu_name', 'RTX 3060'), ('verified', False),
])
def test_frozen_watch_filters_never_substitute_offer(armed, field, value):
    """Price, advertised power, reliability, type and verification are hard gates."""
    _queue, watch = armed
    offer = compatible_offer()
    offer[field] = value
    assert not watching.offer_matches(watch, offer, [])
    assert watching.offer_matches(watch, compatible_offer(), [])


def test_watch_polls_and_claims_one_match(armed, monkeypatch):
    """No match waits; a later matching offer starts the authorized attempt."""
    queue, watch = armed
    rows = []
    monkeypatch.setattr(watching, 'search_host_offers', lambda *args, **kwargs: rows)
    calls = []
    def start(offer, hours, budget, idle_seconds, **kwargs):
        """Record the rechecked claim instead of renting."""
        calls.append((offer['id'], hours, budget, kwargs))
        queue.update(calibration_watch=None)
        return {'id': 'worker'}
    monkeypatch.setattr(queue, 'start', start)
    assert watching.watch_step(queue) is None
    assert queue.read()['calibration_watch']['next_check_at'] > time.time()
    rows.append(compatible_offer())
    queue.update(calibration_watch=dict(watch, next_check_at=0))
    assert watching.watch_step(queue) == {'id': 'worker'}
    assert calls == [(42, 1, 5, {'calibration_id': watch['job_id'],
                                  'calibration_watch_id': watch['id']})]
    assert watching.watch_step(queue) is None
    assert len(calls) == 1


def test_watch_retries_only_after_verified_noncreation(armed, monkeypatch):
    """A 410 can retry after verified absence, never while creation is pending."""
    queue, watch = armed
    offers = [compatible_offer()]
    attempts = []
    monkeypatch.setattr(watching, 'search_host_offers', lambda *args, **kwargs: offers)
    monkeypatch.setattr(watching, 'VastClient', lambda key: SimpleNamespace(
        offers=lambda **kwargs: offers))
    monkeypatch.setattr('vast_credentials.VastCredentialStore', lambda root: SimpleNamespace(
        metadata=lambda: {'generation': 1}))

    def start(identifier, offer, hours, budget):
        """Simulate a durable pending intent without contacting Vast."""
        attempts.append(offer['id'])
        return queue.store.update(identifier, status='provisioning',
                                  rental_state='creation_pending', offer_id=offer['id'])

    monkeypatch.setattr(queue.store, 'start', start)
    first = watching.watch_step(queue)
    assert first['rental_state'] == 'creation_pending'
    assert queue.read()['calibration_watch']['id'] == watch['id']
    assert watching.watch_step(queue) is None
    assert attempts == [42]

    # The independent guard has completed its grace period and two fresh
    # absence checks: Vast did not create a paid instance for the stale offer.
    queue.store.update(first['id'], rental_state='deletion_verified', status='failed',
                       creation_error='Vast offer is no longer available (HTTP 410)',
                       rental_end_reason='provider_creation_failed')
    offers[:] = [dict(compatible_offer(), id=44, machine_id=45)]
    second = watching.watch_step(queue)
    assert second['rental_state'] == 'creation_pending'
    assert attempts == [42, 44]
    assert queue.read()['calibration_watch']['id'] == watch['id']

    queue.store.update(second['id'], rental_state='active', instance_id=123)
    assert watching.watch_step(queue) is None
    assert queue.read()['calibration_watch'] is None
    assert attempts == [42, 44]


def test_watch_survives_supervisor_start_error_with_pending_create(armed, monkeypatch):
    """A local start exception cannot discard consent before Vast resolves PUT."""
    queue, watch = armed
    monkeypatch.setattr(watching, 'search_host_offers', lambda *args, **kwargs: [compatible_offer()])
    monkeypatch.setattr('vast_credentials.VastCredentialStore', lambda root: SimpleNamespace(
        metadata=lambda: {'generation': 1}))

    def interrupted_start(identifier, offer, hours, budget):
        """Model a launched guard and a failing local run supervisor."""
        queue.store.update(identifier, status='provisioning', rental_state='creation_pending')
        raise VastError('Local cloud supervisor could not be started', 503)

    monkeypatch.setattr(queue.store, 'start', interrupted_start)
    assert watching.watch_step(queue) is None
    assert queue.read()['calibration_watch']['id'] == watch['id']
    assert len(queue.workers()) == 1


def test_confirmed_rental_consumes_watch_even_after_fast_cleanup(armed, monkeypatch):
    """A contract observed between polls must never trigger another paid rental."""
    queue, watch = armed
    identifier = 'b' * 32
    from vast_jobs import write_json
    directory = queue.root / 'jobs' / identifier
    directory.mkdir(parents=True)
    write_json(directory / 'state.json', {'id': identifier, 'kind': 'worker',
        'calibration_job_id': watch['job_id'], 'status': 'completed',
        'rental_state': 'deletion_verified', 'instance_id': 123})
    monkeypatch.setattr(watching, 'search_host_offers', lambda *args, **kwargs:
                        pytest.fail('Confirmed rental searched again'))
    assert watching.watch_step(queue) is None
    assert queue.read()['calibration_watch'] is None


def test_cancel_requests_cleanup_for_unconfirmed_worker(armed):
    """Revoking consent also stops an in-flight unconfirmed create."""
    queue, watch = armed
    identifier = 'b' * 32
    from vast_jobs import write_json
    directory = queue.root / 'jobs' / identifier
    directory.mkdir(parents=True)
    write_json(directory / 'state.json', {'id': identifier, 'kind': 'worker',
        'calibration_job_id': watch['job_id'], 'status': 'provisioning',
        'rental_state': 'creation_pending'})
    write_json(directory / 'control.json', {'stop': False, 'cleanup': False})
    assert watching.cancel_watch(queue, watch['id'], 'User cancelled')
    assert queue.store.read(identifier, 'control.json')['cleanup'] is True
    assert queue.read()['calibration_watch'] is None
    assert queue.store.read(watch['job_id'])['status'] == 'cancelled'


@pytest.mark.parametrize('changed', [
    {'price_hour_usd': .25}, {'gpu_max_power_watts': 300}, {'machine_id': 999},
])
def test_watch_rechecks_exact_offer_before_paid_start(armed, monkeypatch, changed):
    """A stale match cannot authorize a different or newly disqualified offer."""
    queue, watch = armed
    monkeypatch.setattr(watching, 'search_host_offers', lambda *args, **kwargs: [compatible_offer()])
    fresh = dict(compatible_offer(), **changed)
    monkeypatch.setattr(watching, 'VastClient', lambda key: SimpleNamespace(
        offers=lambda **kwargs: [fresh]))
    monkeypatch.setattr(queue, 'start', lambda *args, **kwargs:
                        pytest.fail('Stale offer was rented'))

    assert watching.watch_step(queue) is None
    current = queue.read()['calibration_watch']
    assert current['id'] == watch['id']
    assert 'changed or disappeared' in current['last_error']


def test_cancel_prevents_future_rental(armed, monkeypatch):
    """Explicit cancellation persists before a delayed scheduler can rent."""
    queue, watch = armed
    monkeypatch.setattr(watching, 'search_host_offers', lambda *args, **kwargs: [compatible_offer()])
    assert watching.cancel_watch(queue, watch['id'], 'User cancelled')
    assert queue.read()['calibration_watch'] is None
    assert queue.store.read(watch['job_id'])['status'] == 'cancelled'
    assert watching.watch_step(queue) is None


def test_credential_change_revokes_watch(armed, monkeypatch):
    """Changing the paid account generation invalidates old consent."""
    queue, watch = armed
    monkeypatch.setattr(watching, 'VastCredentialStore', lambda root: SimpleNamespace(
        metadata=lambda: {'generation': 2}, secrets=lambda: {'api_key': 'fake'}))
    assert watching.watch_step(queue) is None
    assert queue.read()['calibration_watch'] is None
    assert queue.store.read(watch['job_id'])['status'] == 'cancelled'


def test_corrupt_persisted_consent_fails_closed(armed):
    """A damaged budget cannot widen an unattended rental authorization."""
    queue, watch = armed
    queue.update(calibration_watch=dict(watch, budget=.01))
    with pytest.raises(VastError, match='authorization is invalid'):
        watching.watch_step(queue)
