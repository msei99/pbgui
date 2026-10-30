"""Offline same-Loop infrastructure replacement with real temporary durable stores."""

from javascript_checks import run_javascript
import asyncio
import copy
import json
import time
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from api.vast import RentalPreferences
from pb8_loop_controller import LoopController
from pb8_loop_gpu_replacement import (GPUReplacement, attempts_used, chain_spending,
    open_replacement, review, validate_rental_authorization)
from pb8_loop_store import LoopStore, optimizer_execution_evidence
from vast_jobs import IMAGE, REVISION, JobStore
from vast_provider import VastError
from vast_queue import CloudQueue
from tests.test_pb8_loop_optimizer import OWNER, config, settings, valid_gpu_config

WORKER, NATIVE, PREDECESSOR, OPERATION = ('d' * 32, 'b' * 32, 'c' * 32, 'e' * 32)


class Backend:
    """Preserve runner evidence without processes, credentials, network or bot files."""
    def __init__(self, queue):
        self.queue, self.prepared = queue, []

    def poll(self, record, job):
        """Read the same authoritative state as the real cloud backend."""
        source = self.queue.store.read(job['backend']['id'])
        return {'status': source['status'], 'started': bool(source.get('dispatch_at')), 'source': source}

    def verify_replacement_input(self, record, job):
        """The fixture's in-memory dataset remains fixed throughout each test."""
        from pb8_loop_backend import LoopBackend
        backend = LoopBackend(self.queue.root.parent.parent, self.queue)
        backend.fingerprint = lambda *_: record['fingerprint']
        backend.verify_replacement_input(record, job)

    def prepare(self, record, job):
        """Publish one stable native identity for each already-reserved input."""
        self.prepared.append(copy.deepcopy(job))
        native = job['operation']
        self.queue.store.publish({'id': native, 'kind': 'optimizer', 'status': 'ready',
            'rental_state': 'none', 'loop_id': record['id'], 'loop_owner': record['owner'],
            'loop_operation': job['operation'], 'workers': 1})
        return {'id': native, 'execution': 'vast'}

    def stop(self, record, job):
        """Cancel a temporary native attempt without starting any process."""
        self.queue.store.update(job['backend']['id'], status='cancelled')


@pytest.fixture
def environment(tmp_path, monkeypatch):
    """Every source, archive, provider intent and control lives under tmp_path."""
    (tmp_path / 'api').mkdir()
    (tmp_path / 'api/serial.txt').write_text('1\n')
    loops = LoopStore(tmp_path / 'data/loop_optimizer')
    queue = CloudQueue(JobStore(tmp_path / 'data/vast'))
    prefs = RentalPreferences(budget=3, hours=4, idle_seconds=-1, gpu_name='').model_dump()
    row = loops.create(OWNER, settings(), valid_gpu_config(), {}, {'pb8': 'fixture'}, [], config()['backtest'])
    original = valid_gpu_config()
    original['optimize']['seed'] = 123456
    job = {'operation': PREDECESSOR, 'name': 'fixture', 'kind': 'optimizer', 'round': 0,
        'config': original, 'overrides': {'BTC': {'live': {'user': 'fixture'}}},
        'optimizer_start': {'seed': 123456, 'population': 'fresh_random'}, 'optimizer_archive': {'effective': 17},
        'candidate': None, 'reason': 'Preserved experiment', 'status': 'provisioning', 'started': False,
        'backend': {'id': NATIVE, 'execution': 'vast'}, 'created_at': time.time() - 10}
    def setup(current):
        current['settings'].update(execution='vast', vast=prefs, max_runs=4, max_rounds=2)
        current.update(phase='optimize', round=0, jobs=[job], baseline={'metrics': {'gain': 1}},
                       best={'metrics': {'gain': 2}, 'assessment': {'simulation_complete': True,
                           'comparable': True, 'hard_targets_met': True},
                           'candidate': {'id': 'incumbent', 'config': copy.deepcopy(original), 'overrides': {}}},
                       history=[], baseline_required=False)
    row = loops.update(OWNER, row['id'], setup)
    now = time.time()
    queue.store.publish({'id': WORKER, 'kind': 'worker', 'rental_state': 'active', 'status': 'running',
        'loop_id': row['id'], 'loop_owner': OWNER, 'active_job': NATIVE, 'workers': 1,
        'instance_id': 11, 'host_machine_id': 101, 'deadline': now + 10800,
        'created_at': now - 600}, intent={'id': WORKER, 'image': IMAGE, 'pb8_revision': REVISION,
        'label': 'pbgui-vast-' + WORKER, 'budget_usd': 3, 'accepted_at': now - 600,
        'deadline': now + 10800, 'transfer_reserve_usd': .2,
        'offer': {'id': 123, 'machine_id': 101, 'price_hour_usd': .1, 'disk_gb': 40}})
    queue.store.publish({'id': NATIVE, 'kind': 'optimizer', 'rental_state': 'none', 'status': 'provisioning',
        'loop_id': row['id'], 'loop_owner': OWNER, 'lease_id': WORKER, 'workers': 1,
        'dispatch_at': now - 10})
    queue.update(gpu_preferences=prefs)
    monkeypatch.setattr(JobStore, 'launch_service', lambda *_: None)
    for module in ('vast_queue', 'vast_jobs', 'vast_job_runner'):
        monkeypatch.setattr(module + '.PROJECT', tmp_path)
    return loops, queue, row, Backend(queue)


def begin(environment):
    """Execute an exact native review through the same durable begin boundary as API/AI."""
    loops, queue, row, backend = environment
    data = review(queue, loops.read(OWNER, row['id']), WORKER)
    service = GPUReplacement(loops, queue, backend)
    row = service.begin(OWNER, row['id'], WORKER, row['control_generation'], data['snapshot_digest'], OPERATION)
    return service, row, data


def advance_to_capacity(environment):
    """Confirm independent provider absence only after collection has been requested."""
    service, row, data = begin(environment)
    loops, queue, _, _ = environment
    row = service.step(row)
    queue.store.update(NATIVE, status='cancelled', error='Rental ended', final_collected=False)
    row = service.step(loops.read(OWNER, row['id']))
    assert open_replacement(row)['step'] == 'deletion'
    queue.store.update(WORKER, rental_state='deletion_verified', absent_checks=2, provider_checked_at=time.time())
    row = service.step(row)
    assert open_replacement(row)['step'] == 'capacity'
    return service, row, data


def test_replacement_intent_replay_archive_and_metadata_preserve_identity(environment):
    """Before Stop, intent is durable and duplicate requests cannot create another operation."""
    loops, queue, before, _ = environment
    service, row, data = begin(environment)
    assert row['status'] == 'replacing_gpu' and row['round'] == 0
    assert not queue.store.read(WORKER, 'control.json')['stop']
    assert service.begin(OWNER, row['id'], WORKER, 0, data['snapshot_digest'], OPERATION) == row
    assert attempts_used(row) == 1
    restored = LoopStore(loops.root).read(OWNER, row['id'])
    assert restored == row
    assert loops.list_metadata()[0]['status'] == 'replacing_gpu'
    header = json.loads(loops.path(OWNER, row['id']).read_text())
    assert 'gpu_replacements' not in header and len(json.dumps(header)) < 65536
    for key in ('id', 'initial_config', 'initial_overrides', 'fingerprint', 'baseline', 'best', 'history', 'rubric'):
        assert row[key] == before[key]
    with pytest.raises(VastError, match='another|progress'):
        service.begin(OWNER, row['id'], WORKER, row['control_generation'], data['snapshot_digest'], 'f' * 32)


def test_no_successor_or_rental_before_two_confirmed_absence_checks(environment):
    """A DELETE response or a single absent listing cannot authorize replacement."""
    service, row, _ = begin(environment)
    loops, queue, _, _ = environment
    queue.store.update(NATIVE, status='cancelled')
    row = service.step(row)
    assert open_replacement(row)['step'] == 'deletion'
    for checks in (0, 1):
        queue.store.update(WORKER, rental_state='deletion_verified', absent_checks=checks)
        current = service.step(loops.read(OWNER, row['id']))
        assert open_replacement(current)['step'] == 'deletion'
        assert len(current['jobs']) == 1


def test_preserved_retry_and_successful_same_round_recovery(environment, monkeypatch):
    """A new physical attempt copies seed, overrides, windows and original archive budget."""
    service, row, _ = advance_to_capacity(environment)
    loops, queue, original, backend = environment
    row = service.step(row)
    successor = row['jobs'][-1]
    for key in ('config', 'overrides', 'optimizer_start', 'optimizer_archive', 'round'):
        assert successor[key] == original['jobs'][0][key]
    assert successor['operation'] != PREDECESSOR and successor['gpu_predecessor'] == PREDECESSOR
    assert attempts_used(row) == 2
    row = service.step(row)
    offer = {'id': 999, 'machine_id': 202, 'price_hour_usd': .1, 'num_gpus': 1,
             'cuda_max_good': 13, 'gpu_name': 'Fixture GPU', 'vram_gb': 24, 'ram_gb': 64,
             'cpu_cores': 16, 'tflops': 100, 'gpu_max_power_watts': 500,
             'disk_gb': 80, 'reliability': 1, 'verified': True}
    monkeypatch.setattr('vast_pool.select_offer', lambda *_: offer)
    rentals = []
    def rent(scoped, chosen, hours, budget, idle_seconds, **kwargs):
        """Publish a single ambiguous provider reservation without renting anything."""
        authorization = kwargs['replacement_authorization']
        validate_rental_authorization(scoped, loops.read(OWNER, row['id']), authorization, offer=chosen)
        rentals.append(authorization['worker_id'])
        queue.store.publish({'id': authorization['worker_id'], 'kind': 'worker', 'status': 'provisioning',
            'rental_state': 'creation_pending', 'loop_id': row['id'], 'loop_owner': OWNER,
            'created_at': time.time()}, intent={'gpu_replacement': authorization})
        assert budget < 3 and hours < 3.01
    monkeypatch.setattr(CloudQueue, 'start', rent)
    row = service.step(row)
    # Recreate owners as an API restart, then reconcile the same paid identity.
    service = GPUReplacement(LoopStore(loops.root), queue, backend)
    row = service.step(service.store.read(OWNER, row['id']))
    assert len(rentals) == 1 and row['status'] == 'replacing_gpu'
    queue.store.update(rentals[0], rental_state='active', host_machine_id=202)
    row = service.step(row)
    assert row['status'] == 'running' and row['round'] == original['round'] and len(row['history']) == 0
    assert row['baseline'] == original['baseline'] and row['best'] == original['best']
    assert row['settings'] == original['settings'] and row['deadline'] == original['deadline']
    evidence = optimizer_execution_evidence(row['jobs'][0], row['settings'], row)
    assert evidence['completion_reason'] == 'gpu_replaced' and evidence['backend_status'] == 'cancelled'
    assert evidence['successor_operation'] == successor['operation'] and evidence['attempt_charged']
    assert evidence['result_count'] is None and evidence['error'] == 'Rental ended'


@pytest.mark.parametrize('action', ['pause', 'stop'])
def test_new_control_generation_rejects_replacement_transition(environment, action):
    """A stale replacement tick cannot revive newer Pause or Stop intent."""
    service, row, _ = begin(environment)
    loops, queue, _, backend = environment
    controller = LoopController(loops.root.parent.parent, store=loops, backend=backend)
    current = controller.action(OWNER, row['id'], action)
    with pytest.raises(VastError, match='newer'):
        service.note(row, 'stale transition')
    assert service.step(current)['status'] == ('paused' if action == 'pause' else 'stopping')
    assert not queue.store.read(WORKER, 'control.json')['stop']
    if action == 'pause':
        resumed = controller.action(OWNER, row['id'], 'resume')
        assert resumed['status'] == 'replacing_gpu'
        assert open_replacement(resumed)['id'] == OPERATION
        assert open_replacement(resumed)['generation'] == resumed['control_generation']


def test_storage_failure_blocks_effect_and_resume_keeps_operation(environment, monkeypatch):
    """A failed reservation cannot request Stop; manifest-only pause retains the valid root."""
    from pb8_loop_storage import LoopStorageError
    service, row, _ = begin(environment)
    loops, queue, _, backend = environment
    original = loops.write
    monkeypatch.setattr(loops, 'write', lambda *_: (_ for _ in ()).throw(LoopStorageError('disk full')))
    with pytest.raises(LoopStorageError):
        service.step(row)
    assert not queue.store.read(WORKER, 'control.json')['stop']
    loops.pause_storage(OWNER, row['id'], LoopStorageError('disk full'))
    assert loops.read(OWNER, row['id'])['status'] == 'paused'
    monkeypatch.setattr(loops, 'write', original)
    controller = LoopController(loops.root.parent.parent, store=loops, backend=backend)
    resumed = controller.action(OWNER, row['id'], 'resume')
    assert resumed['status'] == 'replacing_gpu' and open_replacement(resumed)['id'] == OPERATION


def test_limits_provider_error_and_deadline_remain_recoverable(environment, monkeypatch):
    """Limits and provider failures are visible infrastructure waits, never search failures."""
    service, row, _ = advance_to_capacity(environment)
    loops, queue, _, _ = environment
    row = loops.update(OWNER, row['id'], lambda current: current['settings'].update(max_runs=1))
    row = service.step(row)
    assert 'attempt allowance' in row['reason'] and len(row['jobs']) == 1
    row = loops.update(OWNER, row['id'], lambda current: (current['settings'].update(max_runs=4), open_replacement(current).update(retry_at=0)))
    row = service.step(row)
    row = service.step(row)
    monkeypatch.setattr('vast_pool.select_offer', lambda *_: (_ for _ in ()).throw(VastError('No eligible offer; waiting', 409)))
    row = service.step(row)
    assert row['status'] == 'replacing_gpu' and 'No eligible offer' in row['reason']
    assert open_replacement(row)['error']['step'] == 'rent'
    row = loops.update(OWNER, row['id'], lambda current: current.update(deadline=time.time() - 1))
    assert service.step(row)['status'] == 'stopping'


def test_unknown_costs_reserve_complete_original_budget(environment):
    """Unknown price evidence cannot create a fictitious free replacement allowance."""
    _, queue, _, _ = environment
    intent = queue.store.read(WORKER, 'intent.json')
    from vast_jobs import write_json
    intent['offer'].pop('price_hour_usd')
    write_json(queue.store.directory(WORKER) / 'intent.json', intent)
    assert chain_spending(queue, [WORKER]) == 3


@pytest.mark.parametrize('updates,reserve,billed', [
    ({'transfer_reserve_confirmed': 1.2}, 1.2, None),
    ({'transfer_reserved_used': 1.4}, 1.4, None),
    ({'deadline_request': {'transfer_reserve_usd': 1.6}}, 1.6, None),
    ({'transfer_reserve_confirmed': 1.2, 'transfer_reserved_used': 1.4}, 1.4, None),
    ({'transfer_reserve_confirmed': .1}, .2, None),
    ({'transfer_reserve_confirmed': 1.2, 'billing': {'amount_usd': .3}}, 1.2, .3),
    ({'transfer_reserve_confirmed': 1.2, 'billing': {'amount_usd': 2.2}}, 1.2, 2.2),
])
def test_chain_costs_and_review_retain_transfer_reservations_after_restart(environment, monkeypatch, updates, reserve, billed):
    """All persisted transfer obligations reduce one unchanged chain budget, even without billing."""
    loops, queue, row, _ = environment
    intent = queue.store.read(WORKER, 'intent.json')
    now = intent['accepted_at'] + 600
    monkeypatch.setattr('pb8_loop_gpu_replacement.time.time', lambda: now)
    queue.store.update(WORKER, **updates)
    expected = max(reserve + 600 / 3600 * .1, billed or 0)
    assert chain_spending(queue, [WORKER]) == pytest.approx(expected)
    recovered = CloudQueue(JobStore(queue.store.root))
    assert chain_spending(recovered, [WORKER]) == pytest.approx(expected)
    if updates.get('deadline_request'):
        with pytest.raises(VastError, match='current rental change'):
            review(recovered, row, WORKER)
    else:
        preview = review(recovered, row, WORKER)
        assert preview['remaining_budget_usd'] == pytest.approx(3 - expected)
    assert loops.read(OWNER, row['id']) == row
    assert not recovered.read()['gpu_preferences']['auto_rent']


@pytest.mark.parametrize('updates', [
    {'transfer_reserve_confirmed': -1},
    {'transfer_reserved_used': 'unknown'},
    {'deadline_request': {'transfer_reserve_usd': 'unknown'}},
    {'deadline_request': 'unreadable'},
    {'billing': {'amount_usd': 'unknown'}},
])
def test_invalid_transfer_costs_reserve_full_budget_instead_of_free_capacity(environment, updates):
    """Malformed optional evidence cannot make a pending provider bill look cheaper."""
    _, queue, _, _ = environment
    queue.store.update(WORKER, **updates)
    assert chain_spending(queue, [WORKER]) == 3


def test_transfer_costs_accumulate_across_deleted_replacement_rentals(environment, monkeypatch):
    """A second physical lease cannot refund confirmed or consumed transfers from its predecessor."""
    from pb8_loop_gpu_replacement import remaining_limits
    _, queue, _, _ = environment
    now = queue.store.read(WORKER, 'intent.json')['accepted_at'] + 600
    monkeypatch.setattr('pb8_loop_gpu_replacement.time.time', lambda: now)
    queue.store.update(WORKER, transfer_reserve_confirmed=1.2)
    service, row, intent = authorized_lease(environment)
    queue.store.update(intent['id'], rental_state='deletion_verified', absent_checks=2,
        provider_checked_at=now + 1800, transfer_reserved_used=.6)
    row = service.change(row, lambda current, op: op['chain'].append(intent['id']))
    operation = open_replacement(row)
    expected = 1.2 + 600 / 3600 * .1 + .6 + 1800 / 3600 * .1
    recovered = CloudQueue(JobStore(queue.store.root))
    assert chain_spending(recovered, operation['chain']) == pytest.approx(expected)
    limits, _ = remaining_limits(recovered, row, operation)
    assert limits['budget'] == pytest.approx(3 - expected)


def test_open_operation_blocks_normal_sync_and_strategy_evaluation(environment, monkeypatch):
    """An archived open operation is authoritative even after Pause changes visible status."""
    service, row, _ = begin(environment)
    loops, queue, _, backend = environment
    controller = LoopController(loops.root.parent.parent, store=loops, backend=backend)
    monkeypatch.setattr(controller, 'decision', lambda *_: pytest.fail('Infrastructure is not strategy evidence'))
    monkeypatch.setattr(controller, 'job', lambda *_: pytest.fail('Preserved retry must not regenerate its seed'))
    monkeypatch.setattr(controller, 'finish', lambda *_: pytest.fail('No training completion during replacement'))
    assert asyncio.run(controller.sync_jobs(row)) == row
    paused = controller.action(OWNER, row['id'], 'pause')
    asyncio.run(controller._tick(paused))
    assert not backend.prepared and not queue.store.read(WORKER, 'control.json')['stop']


def test_review_ownership_stale_snapshot_and_end_ack(environment):
    """Cross-owner or stale reviews cannot request replacement or silently end attached work."""
    from api.vast import EndRentalAck, _require_loop_end_ack
    loops, queue, row, backend = environment
    with pytest.raises(VastError, match='belong'):
        review(queue, dict(row, owner='f' * 32), WORKER)
    data = review(queue, row, WORKER)
    loops.update(OWNER, row['id'], lambda current: current.update(control_generation=1))
    with pytest.raises(VastError, match='changed'):
        GPUReplacement(loops, queue, backend).begin(OWNER, row['id'], WORKER, 0, data['snapshot_digest'], OPERATION)
    with pytest.raises(VastError, match='can fail'):
        _require_loop_end_ack([queue.store.read(WORKER)], None)
    _require_loop_end_ack([queue.store.read(WORKER)], EndRentalAck(loop_ids=[row['id']]))


def reserved_successor(environment):
    """Reserve and prepare one retry, without contacting a provider."""
    service, row, data = advance_to_capacity(environment)
    row = service.step(row)
    row = service.step(row)
    assert open_replacement(row)['step'] == 'rent'
    return service, row, data


@pytest.mark.parametrize('status', ['replacing_gpu', 'paused'])
def test_original_deadline_also_stops_paused_replacement(environment, status):
    """Pause cannot turn the original deadline into an unlimited rental allowance."""
    service, row, _ = begin(environment)
    loops, queue, _, _ = environment
    row = loops.update(OWNER, row['id'], lambda current: current.update(status=status, deadline=time.time() - 1))
    assert service.step(row)['status'] == 'stopping'
    assert not queue.store.read(WORKER, 'control.json')['stop']


@pytest.mark.parametrize('change', ['seed', 'overrides', 'start', 'archive', 'data', 'pb8'])
def test_frozen_input_and_fingerprint_checked_before_preparation_and_create(environment, change):
    """Changed input or data blocks paid work while retaining the same recoverable operation."""
    from pb8_loop_backend import LoopBackend
    service, row, _ = advance_to_capacity(environment)
    loops, queue, _, _ = environment
    row = service.step(row)
    original = copy.deepcopy(row)
    backend = LoopBackend(queue.root.parent.parent, loops)
    fingerprint = copy.deepcopy(row['fingerprint'])
    backend.fingerprint = lambda *_: fingerprint
    backend.verify_replacement_input(row, row['jobs'][-1])
    if change in {'data', 'pb8'}:
        fingerprint[change] = 'changed'
    else:
        def corrupt(current):
            job = current['jobs'][-1]
            if change == 'seed':
                job['config']['optimize']['seed'] += 1
            elif change == 'overrides':
                job['overrides']['BTC']['live']['user'] = 'different'
            elif change == 'start':
                job['optimizer_start']['seed'] += 1
            else:
                job['optimizer_archive']['effective'] += 1
        row = loops.update(OWNER, row['id'], corrupt)
    service.backend.verify_replacement_input = backend.verify_replacement_input
    current = service.step(row)
    assert current['status'] == 'replacing_gpu' and 'changed' in current['reason']
    assert open_replacement(current)['error']['step'] == 'prepare'
    assert current['baseline'] == original['baseline'] and current['best'] == original['best']
    assert len(queue.store.list()) == 2


def test_reservation_duration_does_not_expire_after_one_second(environment, monkeypatch):
    """Elapsed setup time shortens remaining time, never invalidates or extends the fixed deadline."""
    service, row, _ = reserved_successor(environment)
    loops, queue, _, _ = environment
    from pb8_loop_gpu_replacement import remaining_limits
    operation = open_replacement(row)
    limits, deadline = remaining_limits(queue, row, operation)
    offer = {'machine_id': 202, 'price_hour_usd': .1, 'num_gpus': 1, 'cuda_max_good': 13,
             'gpu_name': 'Fixture GPU', 'vram_gb': 24, 'ram_gb': 64, 'cpu_cores': 16,
             'tflops': 100, 'gpu_max_power_watts': 500, 'disk_gb': 80, 'reliability': 1, 'verified': True}
    authorization = dict(operation_id=operation['id'], generation=row['control_generation'],
        worker_id=operation['worker_id'], job_id=row['jobs'][-1]['backend']['id'],
        hours=limits['hours'], budget=limits['budget'], deadline=deadline, offer=offer)
    row = service.change(row, lambda current, op: op.update(reservation=authorization))
    now = time.time()
    monkeypatch.setattr('pb8_loop_gpu_replacement.time.time', lambda: now + 20)
    validate_rental_authorization(queue, loops.read(OWNER, row['id']), authorization)
    assert row['deadline'] == environment[2]['deadline']


@pytest.mark.parametrize('stage', ['select', 'evaluate'])
def test_actual_strategy_evidence_retains_infrastructure_status_and_attempts(environment, monkeypatch, stage):
    """Capture the actual decision boundary, including Select before any CPU validation."""
    from pb8_loop_controller import owned_thread
    from tests.test_pb8_loop_optimizer import FakeBackend, RULE
    service, row, _ = reserved_successor(environment)
    loops, queue, _, _ = environment
    predecessor, successor = row['jobs']
    def completed(current):
        current.update(status='running', rubric=RULE, phase=stage, observer_enabled=False)
        current['settings'].update(jev_enabled=False)
        current['gpu_replacements'][OPERATION]['step'] = 'done'
        current['jobs'][0].update(status='cancelled', error='Infrastructure interruption', attempt_charged=True)
        current['jobs'][1].update(status='complete', started=True)
        candidate = dict(id='retry-result', config=successor['config'], overrides=successor['overrides'],
                         optimizer_job=successor['operation'], metrics={'gain': 2})
        current['candidates'] = [candidate]
        if stage == 'evaluate':
            validation = dict(operation='f' * 32, kind='validation', round=0, status='complete', candidate=candidate)
            current['jobs'].append(validation)
            current['observations'] = [dict(operation=validation['operation'], exact=True, metrics={'gain': 2},
                candidate=candidate, assessment={'simulation_complete': True, 'comparable': True, 'hard_targets_met': True})]
    row = loops.update(OWNER, row['id'], completed)
    controller = LoopController(queue.root.parent.parent, store=loops, backend=FakeBackend())
    captured = []
    class Captured(Exception):
        """Stop immediately after receiving the produced decision input."""
    async def decision(current, actual_stage, evidence):
        """Capture evidence before any implementation of a model can rewrite it."""
        assert actual_stage == stage
        captured.append(copy.deepcopy(evidence))
        raise Captured()
    monkeypatch.setattr(controller, 'decision', decision)
    with pytest.raises(Captured):
        asyncio.run(controller._tick(row))
    evidence = captured[0]
    old = next(job for job in evidence['jobs'] if job['operation'] == predecessor['operation'])
    assert old['completion_reason'] == 'gpu_replaced' and old['backend_status'] == 'cancelled'
    assert old['successor_operation'] == successor['operation'] and old['attempt_charged']
    assert old['result_count'] is None and old['error'] == 'Infrastructure interruption'
    assert next(job for job in evidence['jobs'] if job['operation'] == successor['operation'])['completion_reason'] is None
    if stage == 'evaluate':
        assert evidence['optimizer_attempts'] == 2 and evidence['remaining_runs'] == 2


@pytest.mark.parametrize('partial_results', [False, True])
def test_replacement_error_is_not_strategy_repair_but_real_results_survive(environment, partial_results):
    """Retain predecessor artifacts and real successor errors without confusing infrastructure with search."""
    from tests.test_pb8_loop_optimizer import FakeBackend, RULE
    service, row, _ = reserved_successor(environment)
    loops, queue, _, _ = environment
    def initialize(current):
        current.update(status='running', phase='optimize', rubric=RULE, observer_enabled=False)
        current['gpu_replacements'][OPERATION]['step'] = 'done'
        current['jobs'][0].update(status='cancelled', started=True, error='old infrastructure error')
        current['jobs'][1].update(status='failed', started=True, error='real successor failure')
    row = loops.update(OWNER, row['id'], initialize)
    class Results(FakeBackend):
        """Only the cancelled predecessor may have retained exact results."""
        def candidates(self, current, job):
            """No missing predecessor artifact becomes a strategy repair error."""
            if partial_results and job['operation'] == PREDECESSOR:
                return super().candidates(current, job)
            raise ValueError('missing artifacts for ' + job['operation'])
    controller = LoopController(queue.root.parent.parent, store=loops, backend=Results())
    asyncio.run(controller._tick(row))
    current = loops.read(OWNER, row['id'])
    if partial_results:
        assert current['phase'] == 'select' and current['candidates'][0]['optimizer_job'] == PREDECESSOR
        assert current['jobs'][0]['result_count'] == 1
    else:
        assert current['phase'] == 'evaluate'
        assert 'real successor failure' in current['repair_error']
        assert 'old infrastructure error' not in current['repair_error'] and PREDECESSOR not in current['repair_error']
    assert current['best'] == row['best']


@pytest.fixture
def replacement_dispatch(environment, monkeypatch):
    """A provider-confirmed temporary lease waits for its preserved optimizer input."""
    from pb8_loop_backend import LoopBackend
    service, row, _ = reserved_successor(environment)
    loops, queue, _, backend = environment
    operation = open_replacement(row)
    worker_id = operation['worker_id']
    native_id = row['jobs'][-1]['backend']['id']
    queue.store.publish(dict(id=worker_id, kind='worker', status='reserved', rental_state='active',
        loop_id=row['id'], loop_owner=OWNER, workers=1, allocated_cpus=1,
        instance_id=22, deadline=time.time() + 10000, gpu_replacement_job=native_id),
        intent=dict(image=IMAGE, pb8_revision=REVISION,
            offer={'download_gb_usd': 0, 'upload_gb_usd': .01}, transfer_reserve_usd=.2))
    row = service.change(row, lambda current, op: (op.update(step='done'), current.update(status='running')))
    monkeypatch.setattr('vast_queue.preflight_gpu_snapshots', lambda store, jobs, **_: jobs)
    monkeypatch.setattr('vast_queue.worker_compatible_jobs', lambda store, jobs, **_: jobs)
    monkeypatch.setattr(LoopBackend, 'fingerprint', lambda *_: row['fingerprint'])
    return loops, queue, row, backend, worker_id, native_id


@pytest.mark.parametrize('action', ['pause', 'stop', 'storage', 'deadline', 'elapsed_deadline',
    'lease_deadline', 'lease_stop', 'cleanup', 'native_owner', 'worker_binding', 'input', 'pause_resume'])
def test_replacement_dispatch_discards_controls_changed_during_fingerprint(replacement_dispatch, monkeypatch, action):
    """A stale preflight must not claim an optimizer after newer controls or binding changes."""
    from pb8_loop_backend import LoopBackend
    from pb8_loop_storage import LoopStorageError
    from vast_queue import worker_step
    loops, queue, row, backend, worker_id, native_id = replacement_dispatch
    controller = LoopController(loops.root.parent.parent, store=loops, backend=backend)
    clock = [time.time()]
    monkeypatch.setattr('vast_queue.time.time', lambda: clock[0])
    if action == 'lease_deadline':
        queue.store.update(worker_id, deadline=clock[0] + 300)

    def fingerprint(*_):
        """Apply only local isolated controls while the expensive scan is in progress."""
        if action in {'pause', 'stop'}:
            controller.action(OWNER, row['id'], action)
        elif action == 'storage':
            loops.pause_storage(OWNER, row['id'], LoopStorageError('fixture disk full'))
        elif action == 'deadline':
            loops.update(OWNER, row['id'], lambda current: current.update(deadline=clock[0] - 1))
        elif action == 'elapsed_deadline':
            clock[0] = row['deadline'] + 1
        elif action == 'lease_deadline':
            clock[0] += 61  # Cross the rental's collection reserve, with a still-valid Loop.
        elif action in {'lease_stop', 'cleanup'}:
            queue.store.control(worker_id, 'stop' if action == 'lease_stop' else 'cleanup')
        elif action == 'native_owner':
            queue.store.update(native_id, loop_owner='f' * 32)
        elif action == 'worker_binding':
            queue.store.update(worker_id, gpu_replacement_job='f' * 32)
        elif action == 'input':
            loops.update(OWNER, row['id'], lambda current: current['jobs'][-1]['config']['optimize'].update(seed=7))
        elif action == 'pause_resume':
            controller.action(OWNER, row['id'], 'pause')
            controller.action(OWNER, row['id'], 'resume')
        return row['fingerprint']

    monkeypatch.setattr(LoopBackend, 'fingerprint', fingerprint)
    assert worker_step(queue, worker_id) is None
    native, worker = queue.store.read(native_id), queue.store.read(worker_id)
    assert native['status'] == 'ready' and not native.get('dispatch_at') and not native.get('lease_id')
    assert not worker.get('active_job') and not worker.get('transfer_reserved_used')
    current = loops.read(OWNER, row['id'])
    assert current['baseline'] == row['baseline'] and current['best'] == row['best']
    assert current['history'] == row['history'] and current['settings'] == row['settings']
    assert len(current['jobs']) == len(row['jobs'])
    if action in {'pause', 'storage', 'pause_resume'}:
        if current['status'] == 'paused':
            controller.action(OWNER, row['id'], 'resume')
        monkeypatch.setattr(LoopBackend, 'fingerprint', lambda *_: row['fingerprint'])
        assert worker_step(queue, worker_id) == native_id
        assert worker_step(queue, worker_id) == native_id
        assert queue.store.read(worker_id)['transfer_reserved_used'] == pytest.approx(.03)


@pytest.mark.parametrize('action', ['pause', 'stop', 'storage'])
def test_replacement_fingerprint_releases_loop_and_queue_for_concurrent_controls(replacement_dispatch, monkeypatch, action):
    """A real competing thread uses Loop-then-Queue order without waiting for the scan."""
    import threading
    from file_lock import advisory_file_lock
    from pb8_loop_backend import LoopBackend
    from pb8_loop_storage import LoopStorageError
    from vast_queue import worker_step
    loops, queue, row, backend, worker_id, native_id = replacement_dispatch
    controller = LoopController(loops.root.parent.parent, store=loops, backend=backend)
    finished, errors, threads = threading.Event(), [], []

    def control():
        """The same lock order as provider Create must remain available during verification."""
        try:
            with advisory_file_lock(loops.path(OWNER, row['id'])), advisory_file_lock(queue.root / '.queue-lock'):
                if action == 'storage':
                    loops.pause_storage(OWNER, row['id'], LoopStorageError('fixture disk full'))
                else:
                    controller.action(OWNER, row['id'], action)
        except Exception as exc:
            errors.append(exc)
        finally:
            finished.set()

    def fingerprint(*_):
        """The competing control must commit before the delayed scan returns."""
        thread = threading.Thread(target=control, name='fixture-replacement-control', daemon=True)
        threads.append(thread)
        thread.start()
        assert finished.wait(3), 'Fingerprint retained a Loop or Queue lock'
        assert not errors
        return row['fingerprint']

    monkeypatch.setattr(LoopBackend, 'fingerprint', fingerprint)
    try:
        assert worker_step(queue, worker_id) is None
    finally:
        for thread in threads:
            thread.join(3)
            assert not thread.is_alive()
    assert not errors and finished.is_set()
    assert queue.store.read(native_id)['status'] == 'ready'
    assert not queue.store.read(worker_id).get('active_job')


@pytest.mark.parametrize('change', ['deadline', 'lease_stop', 'native_stop', 'native_changed'])
def test_replacement_dispatch_rechecks_final_native_commit_boundary(replacement_dispatch, monkeypatch, change):
    """Even the final compatibility pass cannot consume the remaining deadline or hide Stop."""
    from vast_queue import worker_step
    loops, queue, row, _, worker_id, native_id = replacement_dispatch
    clock, calls = [time.time()], []
    monkeypatch.setattr('vast_queue.time.time', lambda: clock[0])

    def compatible(store, jobs, **_):
        """Apply independent controls or time passage just before the native claim."""
        calls.append(True)
        if len(calls) == 2:
            if change == 'deadline':
                clock[0] = row['deadline'] + 1
            elif change in {'lease_stop', 'native_stop'}:
                store.control(worker_id if change == 'lease_stop' else native_id, 'stop')
            else:
                store.update(native_id, error='New native observation')
        return jobs

    monkeypatch.setattr('vast_queue.worker_compatible_jobs', compatible)
    assert worker_step(queue, worker_id) is None
    assert len(calls) == 2
    native = queue.store.read(native_id)
    assert native['status'] in {'ready', 'cancelled'} and not native.get('dispatch_at')
    assert not queue.store.read(worker_id).get('active_job')


def test_successor_and_worker_are_exclusive_without_full_budget_fallback(environment, monkeypatch):
    """Parallel jobs cannot steal the reserved lease or run its preserved input elsewhere."""
    from vast_queue import worker_step
    from pb8_loop_backend import LoopBackend
    from vast_jobs import write_json
    service, row, _ = reserved_successor(environment)
    loops, queue, _, _ = environment
    operation = open_replacement(row)
    successor = row['jobs'][-1]
    retry = successor['backend']['id']
    other_worker, other_job = '1' * 32, '2' * 32
    worker_id = operation['worker_id']
    for identifier in (worker_id, other_worker):
        queue.store.publish(dict(id=identifier, kind='worker', status='reserved', rental_state='active',
            loop_id=row['id'], loop_owner=OWNER, workers=1, allocated_cpus=1, deadline=time.time() + 10000,
            gpu_replacement_job=retry if identifier == worker_id else None), intent={
            'image': IMAGE, 'pb8_revision': REVISION, 'offer': {'download_gb_usd': 0, 'upload_gb_usd': .01},
            'transfer_reserve_usd': .2})
    queue.store.publish(dict(id=other_job, kind='optimizer', status='ready', rental_state='none',
        loop_id=row['id'], loop_owner=OWNER, workers=1, created_at=0))
    row = service.change(row, lambda current, op: (op.update(step='done'), current.update(status='running')))
    monkeypatch.setattr('vast_queue.preflight_gpu_snapshots', lambda store, jobs, **_: jobs)
    monkeypatch.setattr('vast_queue.worker_compatible_jobs', lambda store, jobs, **_: jobs)
    monkeypatch.setattr(LoopBackend, 'fingerprint', lambda *_: row['fingerprint'])
    assert worker_step(queue, other_worker) == other_job
    assert queue.store.read(retry)['status'] == 'ready'
    assert worker_step(queue, worker_id) == retry
    assert queue.store.read(retry)['lease_id'] == worker_id
    assert queue.store.read(other_job)['lease_id'] == other_worker
    backend = LoopBackend(queue.root.parent.parent, loops)
    monkeypatch.setattr(backend, '_rent', lambda *_: pytest.fail('Must not rent at original full allowances'))
    monkeypatch.setattr(backend, 'poll', lambda *_: {'status': 'queued'})
    backend._start_locked(row, successor)


@pytest.mark.parametrize('published', [False, True])
def test_stop_reserved_or_ambiguous_successor_waits_for_confirmed_cleanup(environment, monkeypatch, published):
    """Stop cancels unpublished input and waits for a potentially paid replacement lease."""
    from pb8_loop_backend import LoopBackend
    from tests.test_pb8_loop_optimizer import FakeBackend
    service, row, _ = advance_to_capacity(environment)
    loops, queue, _, _ = environment
    row = service.step(row)  # successor is planned, without a backend
    operation = open_replacement(row)
    worker_id = operation['worker_id']
    if published:
        queue.store.publish(dict(id=worker_id, kind='worker', status='provisioning', rental_state='creation_pending',
            loop_id=row['id'], loop_owner=OWNER), intent={})
        from vast_jobs import write_json
        write_json(queue.store.directory(worker_id) / 'attempt.json', {'attempted_at': time.time()})
    class Cleanup(FakeBackend):
        """Use real replacement lease settlement, with a simulated native Stop/Poll."""
        replacement_cleanup_pending = LoopBackend.replacement_cleanup_pending
        def poll(self, current, job):
            """A native cancellation alone does not prove provider absence."""
            return {'status': 'cancelled', 'started': True}
    backend = Cleanup()
    monkeypatch.setattr('vast_jobs.JobStore', lambda: queue.store)
    controller = LoopController(queue.root.parent.parent, store=loops, backend=backend)
    row = controller.action(OWNER, row['id'], 'stop')
    asyncio.run(controller._tick(row))
    current = loops.read(OWNER, row['id'])
    assert current['jobs'][-1]['status'] == 'cancelled'
    assert current['cleanup_done'] is (not published)
    if published:
        assert open_replacement(current)
        assert queue.store.read(worker_id, 'control.json')['cleanup']
        queue.store.update(worker_id, rental_state='deletion_verified', absent_checks=2)
        asyncio.run(controller._tick(current))
        current = loops.read(OWNER, row['id'])
        assert current['cleanup_done'] and not open_replacement(current)


def test_authenticated_api_review_ownership_authorization_and_replay(environment, monkeypatch):
    """The actual routes reject foreign owners and stale/unauthorized requests before intent."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api import loop_optimizer_v8 as api
    loops, queue, row, backend = environment
    controller = LoopController(queue.root.parent.parent, store=loops, backend=backend)
    monkeypatch.setattr(api, '_controller', controller)
    monkeypatch.setattr('vast_queue.CloudQueue', lambda: queue)
    monkeypatch.setattr(api, '_owner', lambda session: session.user_id)
    app = FastAPI()
    app.include_router(api.router, prefix='/loops')
    app.dependency_overrides[api.require_auth] = lambda: SimpleNamespace(user_id=OWNER)
    path = '/loops/' + row['id'] + '/rental/'
    with TestClient(app) as client:
        preview = client.post(path + 'replace-preview', json={'worker_id': WORKER})
        assert preview.status_code == 200 and preview.headers['cache-control'] == 'no-store'
        data = preview.json()
        body = dict(worker_id=WORKER, expected_generation=data['snapshot']['generation'],
            snapshot_digest=data['snapshot_digest'], operation_id=data['operation_id'])
        assert client.post(path + 'replace', json=body).status_code == 422
        app.dependency_overrides[api.require_auth] = lambda: SimpleNamespace(user_id='f' * 32)
        assert client.post(path + 'replace-preview', json={'worker_id': WORKER}).status_code == 404
        assert client.post(path + 'replace', json=dict(body, authorization=True)).status_code == 404
        app.dependency_overrides[api.require_auth] = lambda: SimpleNamespace(user_id=OWNER)
        response = client.post(path + 'replace', json=dict(body, authorization=True))
        assert response.status_code == 202 and response.json()['status'] == 'replacing_gpu'
        assert client.post(path + 'replace', json=dict(body, authorization=True)).status_code == 202
        assert len(loops.read(OWNER, row['id'])['gpu_replacements']) == 1
        assert not queue.store.read(WORKER, 'control.json')['stop']


@pytest.mark.parametrize('interrupted', [False, True])
def test_native_approval_replay_and_api_restart_recover_same_intent(environment, tmp_path, monkeypatch, interrupted):
    """Only explicit owned approval can publish intent; recovery acknowledges the committed operation."""
    from ai_capabilities import AICapabilityService, AICapabilityError
    from api import loop_optimizer_v8 as api
    loops, queue, row, backend = environment
    monkeypatch.setattr(api, '_controller', LoopController(queue.root.parent.parent, store=loops, backend=backend))
    monkeypatch.setattr('vast_queue.CloudQueue', lambda: queue)
    service = AICapabilityService(tmp_path / 'ai')
    async def scenario():
        """Persist a real proposal and its approved execution under isolated credentials-free stores."""
        proposal = await service.dispatch(OWNER, '3' * 32, 'propose_ai_loop_gpu_replacement',
                                         {'loop_id': row['id'], 'worker_id': WORKER})
        saved = service.proposals[proposal['proposal_id']]
        assert not loops.read(OWNER, row['id']).get('gpu_replacements')
        with pytest.raises(AICapabilityError):
            await service.approve('f' * 32, saved.id, saved.payload_digest, saved.conversation_id)
        with pytest.raises(AICapabilityError):
            await service.approve(OWNER, saved.id, saved.payload_digest, '4' * 32)
        result = await service.approve(OWNER, saved.id, saved.payload_digest, saved.conversation_id)
        assert result['status'] == 'executed' and loops.read(OWNER, row['id'])['status'] == 'replacing_gpu'
        if interrupted:
            saved.status, saved.result = 'executing', None
            service._persist_proposal(saved)  # crash after intent, before proposal acknowledgement
        restored = AICapabilityService(tmp_path / 'ai')
        await restored.startup()
        replay = await restored.approve(OWNER, saved.id, saved.payload_digest, saved.conversation_id)
        assert replay['operation_id'] == result['operation_id']
        assert len(loops.read(OWNER, row['id'])['gpu_replacements']) == 1
    asyncio.run(scenario())


@pytest.mark.parametrize('batch', [False, True])
def test_both_end_rental_routes_require_exact_loop_acknowledgement(environment, monkeypatch, batch):
    """Ordinary End rental stays distinct, including when ending all selected rentals."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api import vast
    _, queue, row, _ = environment
    effects = []
    monkeypatch.setattr(vast, 'CloudQueue', lambda: queue)
    monkeypatch.setattr(queue, 'worker_action', lambda *args: effects.append(args) or {})
    monkeypatch.setattr(queue, 'action', lambda *args: effects.append(args) or {})
    app = FastAPI()
    app.include_router(vast.router)
    app.dependency_overrides[vast.require_auth] = lambda: SimpleNamespace(user_id=OWNER)
    path = '/queue/end' if batch else '/queue/workers/' + WORKER + '/end'
    with TestClient(app) as client:
        assert client.post(path).status_code == 409
        assert client.post(path, json={'loop_ids': ['f' * 32]}).status_code == 409
        assert not effects
        assert client.post(path, json={'loop_ids': [row['id']]}).status_code == (200 if batch else 202)
        assert len(effects) == 1


def test_observer_waiting_jobs_survive_replacement_and_running_reports_are_collected(environment):
    """Replacement neither starts waiting reports nor discards independently running reports."""
    from tests.test_pb8_loop_observer import Backend as ObserverBackend, reference
    from pb8_loop_controller import owned_thread
    from pb8_loop_observer import ObserverJobs
    loops, queue, row, _ = environment
    # Plan user-only jobs using the real planner, then retain one already-running report.
    row = reference(loops, row)
    class Reports(ObserverBackend):
        """Only a report that actually started can finish in this fixture."""
        def poll(self, current, job):
            """Queued jobs remain queued even while another report completes."""
            if job['operation'] not in self.starts:
                return {'status': 'queued', 'started': False}
            return super().poll(current, job)
    backend = Reports()
    observer = ObserverJobs(loops, backend, owned_thread)
    row = observer.plan(row)
    waiting = row['observer_jobs'][0]['operation']
    queued = row['observer_jobs'][1]['operation']
    running = row['observer_jobs'][2]['operation']
    backend.starts.append(running)
    def replacing(current):
        current.update(status='replacing_gpu', gpu_replacement_id=OPERATION,
            gpu_replacements={OPERATION: {'id': OPERATION, 'step': 'deletion'}})
        current['observer_jobs'][1].update(backend={'id': queued, 'execution': 'validation'}, status='queued')
        current['observer_jobs'][2].update(backend={'id': running, 'execution': 'validation'}, status='running', started=True)
    row = loops.update(OWNER, row['id'], replacing)
    backend.done = True
    asyncio.run(observer.tick(row))
    current = loops.read(OWNER, row['id'])
    jobs = {job['operation']: job for job in current['observer_jobs']}
    assert jobs[waiting]['status'] == 'planned' and not jobs[waiting].get('report_done')
    assert jobs[queued]['status'] == 'queued' and not jobs[queued].get('report_done')
    assert jobs[running]['report_done'] and jobs[running]['simulation_complete']
    assert backend.starts == [running] and not backend.stops
    assert current['best'] == row['best'] and current['history'] == row['history']
    current = loops.update(OWNER, row['id'], lambda value: (value.update(status='running'),
        value['gpu_replacements'][OPERATION].update(step='done')))
    asyncio.run(observer.tick(current))
    current = loops.read(OWNER, row['id'])
    assert {waiting, queued} <= set(backend.starts)


@pytest.mark.parametrize('waiting_status', ['planned', 'queued'])
@pytest.mark.parametrize('status', ['replacing_gpu', 'paused'])
@pytest.mark.parametrize('restart', [False, True])
def test_replacement_observer_backlog_cannot_starve_running_report(environment, waiting_status, status, restart):
    """Four preserved waiters cannot hide a later running report during replacement or Pause."""
    from tests.test_pb8_loop_observer import Backend as ObserverBackend, reference
    from pb8_loop_controller import owned_thread
    from pb8_loop_observer import ObserverJobs
    loops, queue, row, _ = environment
    row = reference(loops, row, rounds=2)

    class Reports(ObserverBackend):
        """Keep waiting native jobs queued while one independent simulation finishes."""
        def __init__(self):
            """Own a small trace of simulated native polls for this test only."""
            super().__init__()
            self.polls = []

        def poll(self, current, job):
            """Record actual native reads, including ones that discover later completion."""
            self.polls.append(job['operation'])
            if job['operation'] not in self.starts:
                return {'status': 'queued', 'started': False}
            return super().poll(current, job)

    backend = Reports()
    observer = ObserverJobs(loops, backend, owned_thread)
    row = observer.plan(row)
    waiting = [job['operation'] for job in row['observer_jobs'][:4]]
    running = row['observer_jobs'][4]['operation']
    backend.starts.append(running)

    def replacing(current):
        current.update(status=status, gpu_replacement_id=OPERATION,
            gpu_replacements={OPERATION: {'id': OPERATION, 'step': 'deletion',
                'generation': current['control_generation']}})
        if waiting_status == 'queued':
            for job in current['observer_jobs'][:4]:
                job.update(status='queued', backend={'id': job['operation'], 'execution': 'validation'})
        current['observer_jobs'][4].update(backend={'id': running, 'execution': 'validation'},
            status='running', started=True)

    row = loops.update(OWNER, row['id'], replacing)
    for _ in range(2):
        before = len(backend.polls)
        asyncio.run(observer.tick(loops.read(OWNER, row['id'])))
        assert len(backend.polls) - before <= 4
    assert running in backend.polls
    backend.done = True
    if restart:
        observer = ObserverJobs(LoopStore(loops.root), backend, owned_thread)
    for _ in range(2):
        before = len(backend.polls)
        asyncio.run(observer.tick(loops.read(OWNER, row['id'])))
        assert len(backend.polls) - before <= 4
    current = loops.read(OWNER, row['id'])
    jobs = {job['operation']: job for job in current['observer_jobs']}
    assert jobs[running]['report_done'] and jobs[running]['simulation_complete']
    assert all(jobs[operation]['status'] == waiting_status
        and not jobs[operation].get('report_done') for operation in waiting)
    assert backend.starts == [running] and not backend.stops
    for field in ('status', 'settings', 'initial_config', 'fingerprint', 'baseline', 'best', 'history'):
        assert current[field] == row[field]

    controller = LoopController(queue.root.parent.parent, store=loops, backend=backend)
    if current['status'] != 'paused':
        controller.action(OWNER, row['id'], 'pause')
    resumed = controller.action(OWNER, row['id'], 'resume')
    assert resumed['status'] == 'replacing_gpu' and open_replacement(resumed)['id'] == OPERATION
    asyncio.run(observer.tick(resumed))
    assert backend.starts == [running] and not backend.stops
    current = loops.update(OWNER, row['id'], lambda value: (value.update(status='running'),
        value['gpu_replacements'][OPERATION].update(step='done')))
    asyncio.run(observer.tick(current))
    assert set(waiting) <= set(backend.starts)
    assert backend.starts.count(running) == 1 and not backend.stops


def authorized_lease(environment):
    """Publish a provider-ready replacement reservation without starting a service."""
    from pb8_loop_gpu_replacement import remaining_limits, replacement_gpu_profiles
    service, row, data = reserved_successor(environment)
    loops, queue, _, _ = environment
    operation = open_replacement(row)
    limits, deadline = remaining_limits(queue, row, operation)
    offer = dict(id=999, machine_id=202, price_hour_usd=.1, num_gpus=1, cuda_max_good=13,
        gpu_name='Fixture GPU', vram_gb=24, ram_gb=64, cpu_cores=16, tflops=100,
        gpu_max_power_watts=500, disk_gb=80, reliability=1, verified=True,
        download_gb_usd=0, upload_gb_usd=0)
    authorization = dict(operation_id=operation['id'], generation=row['control_generation'],
        worker_id=operation['worker_id'], job_id=row['jobs'][-1]['backend']['id'],
        hours=limits['hours'], budget=limits['budget'], deadline=deadline, offer=offer,
        **replacement_gpu_profiles(operation, row['jobs'][-1]['backend']['id']))
    row = service.change(row, lambda current, op: op.update(reservation=authorization))
    worker_id = operation['worker_id']
    now = time.time()
    intent = dict(id=worker_id, image=IMAGE, pb8_revision=REVISION, label='pbgui-vast-' + worker_id,
        accepted_at=now, deadline=deadline, budget_usd=limits['budget'], offer=offer,
        transfer_reserve_usd=.2, gpu_replacement=authorization,
        **replacement_gpu_profiles(operation, row['jobs'][-1]['backend']['id']))
    queue.store.publish(dict(id=worker_id, kind='worker', status='provisioning', rental_state='creation_pending',
        loop_id=row['id'], loop_owner=OWNER, workers=1, deadline=deadline,
        gpu_replacement_job=authorization['job_id']), intent=intent)
    return service, row, intent


@pytest.mark.parametrize('timing', ['before_reservation', 'after_reservation', 'during_fingerprint'])
def test_paid_create_rechecks_confirmed_transfer_costs_and_retains_reservation(environment, monkeypatch, timing):
    """A fresh guard cannot spend a reservation made before increased chain transfer obligations."""
    from vast_job_runner import guard_step
    from pb8_loop_backend import LoopBackend
    loops, queue, _, backend = environment
    original = queue.store.read(WORKER, 'intent.json')
    now = original['accepted_at'] + 600
    monkeypatch.setattr('pb8_loop_gpu_replacement.time.time', lambda: now)
    if timing == 'before_reservation':
        queue.store.update(WORKER, transfer_reserve_confirmed=1.2, transfer_reserved_used=1.1)
    service, row, intent = authorized_lease(environment)
    worker_id = intent['id']
    if timing == 'after_reservation':
        queue.store.update(WORKER, transfer_reserve_confirmed=1.2, transfer_reserved_used=1.1)
    def fingerprint(*_):
        """Model a late native cost observation during the final slow input verification."""
        if timing == 'during_fingerprint':
            queue.store.update(WORKER, transfer_reserve_confirmed=1.2, transfer_reserved_used=1.1)
        return row['fingerprint']
    monkeypatch.setattr(LoopBackend, 'fingerprint', fingerprint)
    calls = []
    class Provider:
        """No credentials or network; observe only the authoritative paid Create boundary."""
        def request(self, method, path, *_):
            """Record the one permitted request after correct initial cost reservation."""
            calls.append((method, path))
            return {'new_contract':22}
        def instances(self, **_):
            """Return the instance bound to the exact durable reservation."""
            return [dict(id=22, label=intent['label'], machine_id=202, actual_status='running')]
    recovered = JobStore(queue.store.root)
    if timing == 'before_reservation':
        assert intent['budget_usd'] == pytest.approx(3 - 1.2 - 600 / 3600 * .1)
        guard_step(recovered, worker_id, Provider(), intent, '')
        guard_step(JobStore(queue.store.root), worker_id, Provider(), intent, '')
        assert calls == [('PUT', '/asks/999/')]
    else:
        for _ in range(2):
            with pytest.raises(VastError, match='Remaining replacement authorization changed'):
                guard_step(recovered, worker_id, Provider(), intent, '')
        assert calls == [] and not (recovered.directory(worker_id) / 'attempt.json').exists()
        current = service.step(row)
        current = service.step(current)
        assert 'Remaining replacement authorization changed' in current['reason']
        assert current['status'] == 'replacing_gpu'
        assert open_replacement(current)['worker_id'] == worker_id
        assert open_replacement(current)['reservation'] == intent['gpu_replacement']
    current = loops.read(OWNER, row['id'])
    assert current['round'] == row['round'] and current['deadline'] == row['deadline']


def test_ambiguous_replacement_create_is_never_repeated_and_needs_fresh_absence(environment, monkeypatch):
    """A timeout, restart and DELETE acknowledgement never prove that paid capacity is gone."""
    from vast_job_runner import guard_step
    from pb8_loop_backend import LoopBackend
    service, row, intent = authorized_lease(environment)
    loops, queue, _, _ = environment
    identifier = intent['id']
    monkeypatch.setattr(LoopBackend, 'fingerprint', lambda *_: row['fingerprint'])
    calls, instances = [], []
    class Provider:
        """Model the provider protocol without credentials or network."""
        def request(self, method, path, *args):
            """Create is sent once but its response is lost."""
            calls.append((method, path))
            if method == 'PUT':
                raise VastError('Provider response timed out')
            return {}
        def instances(self, **kwargs):
            """Require uncached lists, even across controller recreation."""
            assert kwargs == {'fresh': True}
            return list(instances)
    provider = Provider()
    now = time.time()
    with pytest.raises(VastError, match='timed out'):
        guard_step(queue.store, identifier, provider, intent, '', now=now)
    assert (queue.store.directory(identifier) / 'attempt.json').exists()
    # New restrictions forbid unsent Creates, but must not block reconciliation
    # of a request which may already have created paid capacity.
    queue.set_host_block(202, True)
    queue.store.update(WORKER, transfer_reserve_confirmed=1.2, transfer_reserved_used=1.1)
    queue.store.publish(dict(id='9' * 32, kind='worker', status='running', rental_state='active'))
    # A recreated guard observes the same durable Create marker.
    assert not guard_step(JobStore(queue.store.root), identifier, provider, intent, '', now=now + 30)
    assert not queue.store.read(identifier).get('absent_checks')
    queue.store.control(identifier, 'cleanup')
    instances.append(dict(id=22, label=intent['label'], machine_id=202, actual_status='running'))
    assert not guard_step(queue.store, identifier, provider, intent, '', now=now + 130)
    assert calls == [('PUT', '/asks/999/'), ('DELETE', '/instances/22/')]
    assert queue.store.read(identifier)['rental_state'] == 'destroy_pending'
    instances.clear()
    assert not guard_step(queue.store, identifier, provider, intent, '', now=now + 140)
    assert not guard_step(queue.store, identifier, provider, intent, '', now=now + 145)
    assert guard_step(queue.store, identifier, provider, intent, '', now=now + 150)
    assert queue.store.read(identifier)['rental_state'] == 'deletion_verified'
    assert sum(method == 'PUT' for method, _ in calls) == 1


def test_slow_input_verification_cannot_consume_ambiguous_create_wait(environment, monkeypatch):
    """Start the propagation grace at the paid effect, even after slow local verification."""
    from pb8_loop_backend import LoopBackend
    from vast_job_runner import guard_step
    _, row, intent = authorized_lease(environment)
    _, queue, _, _ = environment
    worker_id = intent['id']
    started_at = time.time()
    clock, calls = [started_at], []
    monkeypatch.setattr('vast_job_runner.time.time', lambda: clock[0])

    def fingerprint(*_):
        """Represent a large local data scan without sleeping or reading runtime data."""
        clock[0] += 180
        return row['fingerprint']

    class Provider:
        """An ambiguous Create stays invisible until the complete propagation grace expires."""
        def request(self, method, path, *_):
            """Record exactly one simulated paid request with a lost response."""
            calls.append((method, path))
            raise VastError('Provider response timed out')

        def instances(self, **kwargs):
            """Every absence observation is fresh and uses no actual provider."""
            assert kwargs == {'fresh': True}
            return []

    monkeypatch.setattr(LoopBackend, 'fingerprint', fingerprint)
    with pytest.raises(VastError, match='timed out'):
        guard_step(queue.store, worker_id, Provider(), intent, '', now=started_at)
    assert queue.store.read(worker_id, 'attempt.json')['attempted_at'] == clock[0]
    # Recreated guards must not count absence during the actual Create's grace,
    # although more than 120 seconds have passed since the first guard entered.
    for elapsed in (190, 200, 299):
        clock[0] = started_at + elapsed
        assert not guard_step(JobStore(queue.store.root), worker_id, Provider(), intent, '')
        assert not queue.store.read(worker_id).get('absent_checks')
    clock[0] = started_at + 300
    assert not guard_step(queue.store, worker_id, Provider(), intent, '')
    assert queue.store.read(worker_id)['absent_checks'] == 1
    clock[0] += 10
    assert guard_step(JobStore(queue.store.root), worker_id, Provider(), intent, '')
    assert queue.store.read(worker_id)['rental_state'] == 'deletion_verified'
    assert calls == [('PUT', '/asks/999/')]


@pytest.mark.parametrize('restriction', ['capacity', 'host'])
@pytest.mark.parametrize('during_fingerprint', [False, True])
def test_reserved_create_rechecks_capacity_and_host_then_recovers_same_identity(environment, monkeypatch, restriction, during_fingerprint):
    """A late restriction blocks the sole paid Create without consuming its durable identity."""
    from vast_job_runner import guard_step
    from pb8_loop_backend import LoopBackend
    loops, queue, initial, _ = environment
    loops.update(OWNER, initial['id'], lambda row: row['settings']['vast'].update(max_rentals=2))
    prefs = queue.read()['gpu_preferences']
    prefs['max_rentals'] = 2
    queue.update(gpu_preferences=prefs)
    service, row, intent = authorized_lease(environment)
    worker_id = intent['id']
    if restriction == 'capacity':
        queue.store.publish(dict(id='9' * 32, kind='worker', status='running', rental_state='active'))
    def restrict():
        """Use the real settings/block transactions, after reservation or during the slow check."""
        if restriction == 'capacity':
            current = queue.read()['gpu_preferences']
            current['max_rentals'] = 1
            queue.update(gpu_preferences=current)
        else:
            queue.set_host_block(202, True)
    if not during_fingerprint:
        restrict()
    pending = [during_fingerprint]
    def fingerprint(*_):
        """A changed requirement during input verification must be rechecked before the marker."""
        if pending[0]:
            pending[0] = False
            restrict()
        return row['fingerprint']
    monkeypatch.setattr(LoopBackend, 'fingerprint', fingerprint)
    calls = []
    class Provider:
        """No provider account, SSH, credentials or real capacity is used."""
        def request(self, method, path, *_):
            """Only the eventual authorized Create is accepted."""
            calls.append((method, path))
            return {'new_contract':22}
        def instances(self, **_):
            """The one created fixture instance has the reserved label and machine."""
            return [dict(id=22, label=intent['label'], machine_id=202, actual_status='running')]
    provider = Provider()
    for _ in range(2):
        with pytest.raises(VastError, match='capacity|blocked'):
            guard_step(queue.store, worker_id, provider, intent, '')
    assert not calls and not (queue.store.directory(worker_id) / 'attempt.json').exists()
    assert queue.store.read(worker_id)['authorization_wait_reason']
    waiting = service.step(row)
    waiting = service.step(waiting)
    assert 'capacity' in waiting['reason'] or 'blocked' in waiting['reason']
    assert open_replacement(waiting)['worker_id'] == worker_id
    prefs['max_rentals'] = 2
    queue.update(gpu_preferences=prefs)
    queue.set_host_block(202, False)
    guard_step(queue.store, worker_id, provider, intent, '')
    guard_step(JobStore(queue.store.root), worker_id, provider, intent, '')
    assert calls == [('PUT', '/asks/999/')]
    assert queue.store.read(worker_id)['authorization_wait_reason'] is None
    assert open_replacement(loops.read(OWNER, row['id']))['worker_id'] == worker_id


def parallel_attempt(environment, row):
    """Attach one dispatched parallel optimizer to temporary native/Loop stores."""
    loops, queue, _, _ = environment
    job = {key: copy.deepcopy(row['jobs'][0][key]) for key in
        ('kind', 'round', 'config', 'overrides', 'optimizer_start', 'optimizer_archive', 'reason', 'candidate')}
    job.update(operation='7' * 32, name='parallel', status='running', started=False,
        run_started_at=time.time() - 7200, backend={'id':'6' * 32, 'execution':'vast'})
    queue.store.publish(dict(id='6' * 32, kind='optimizer', status='running', rental_state='none',
        loop_id=row['id'], loop_owner=OWNER, lease_id='5' * 32, dispatch_at=time.time() - 7200,
        started_at=time.time() - 7200))
    return loops.update(OWNER, row['id'], lambda current: current['jobs'].append(job))


@pytest.mark.parametrize('mode', ['hours', 'proxy'])
@pytest.mark.parametrize('paused', [False, True])
def test_open_replacement_still_limits_parallel_attempt_without_launch_or_evaluation(environment, monkeypatch, mode, paused):
    """Monitor dispatched work through the real controller entry while its successor stays reserved."""
    service, row, _ = reserved_successor(environment)
    loops, queue, original, backend = environment
    row = parallel_attempt(environment, row)
    row = loops.update(OWNER, row['id'], lambda current: (
        current['settings'].update(run_limit_mode=mode, run_hours=1, run_proxy=10),
        open_replacement(current).update(retry_at=time.time() + 600)))
    controller = LoopController(loops.root.parent.parent, store=loops, backend=backend)
    if paused:
        row = controller.action(OWNER, row['id'], 'pause')
    poll, stopped = backend.poll, []
    def observe(current, job):
        """Expose a proxy quota and actual runtime evidence without starting a job."""
        state = poll(current, job)
        if job['operation'] == '7' * 32:
            state.update(proxy_evaluations=20, executed_config=copy.deepcopy(job['config']))
        return state
    stop = backend.stop
    def stop_parallel(current, job):
        """Only the already dispatched parallel attempt may receive a quota Stop."""
        stopped.append(job['operation'])
        assert job['operation'] == '7' * 32
        stop(current, job)
    monkeypatch.setattr(backend, 'poll', observe)
    monkeypatch.setattr(backend, 'stop', stop_parallel)
    monkeypatch.setattr(backend, 'prepare', lambda *_: pytest.fail('No preparation during replacement monitoring'))
    monkeypatch.setattr(backend, 'start', lambda *_: pytest.fail('No normal Queue start during replacement'), raising=False)
    for name in ('decision', 'job', 'finish'):
        monkeypatch.setattr(controller, name, lambda *_: pytest.fail('No training phase during replacement'))
    asyncio.run(controller._tick(row))
    current = loops.read(OWNER, row['id'])
    parallel = next(job for job in current['jobs'] if job['operation'] == '7' * 32)
    assert stopped == ['7' * 32] and parallel['status'] == 'cancelled'
    assert parallel['limit_reached'] == mode and parallel['started']
    assert parallel['proxy_evaluations'] == 20 and parallel['executed_config'] == parallel['config']
    assert parallel['ended_at'] and parallel['runner']['lease_id'] == '5' * 32
    successor = next(job for job in current['jobs'] if job['operation'] == open_replacement(current)['successor'])
    assert successor['status'] == 'queued' and queue.store.read(successor['backend']['id'])['status'] == 'ready'
    assert current['status'] == ('paused' if paused else 'replacing_gpu')
    for key in ('round', 'history', 'best', 'baseline', 'deadline'):
        assert current[key] == original[key]
    # Repeated polling and a fresh controller do not repeat a completed Stop.
    recovered = LoopController(loops.root.parent.parent, store=loops, backend=backend)
    asyncio.run(recovered.sync_jobs(current))
    assert stopped == ['7' * 32]


@pytest.mark.parametrize('action', ['pause', 'stop', 'storage'])
def test_parallel_quota_monitor_discards_newer_control_before_stop(environment, monkeypatch, action):
    """A control arriving during poll invalidates the old quota write and local Stop."""
    from pb8_loop_storage import LoopStorageError
    service, row, _ = reserved_successor(environment)
    loops, _, _, backend = environment
    row = parallel_attempt(environment, row)
    row = loops.update(OWNER, row['id'], lambda current: current['settings'].update(run_limit_mode='hours', run_hours=1))
    controller = LoopController(loops.root.parent.parent, store=loops, backend=backend)
    poll = backend.poll
    def changed(current, job):
        """Serialize a real control transaction while the native observation is in flight."""
        state = poll(current, job)
        if action == 'storage':
            loops.pause_storage(OWNER, row['id'], LoopStorageError('fixture disk full'))
        else:
            controller.action(OWNER, row['id'], action)
        return state
    monkeypatch.setattr(backend, 'poll', changed)
    monkeypatch.setattr(backend, 'stop', lambda *_: pytest.fail('Newer control invalidated the old Stop'))
    current = service.step(row)
    assert current['status'] == ('stopping' if action == 'stop' else 'paused')
    assert current['control_generation'] > row['control_generation']
    parallel = next(job for job in current['jobs'] if job['operation'] == '7' * 32)
    assert parallel['status'] == 'running' and not parallel.get('limit_reached')
    assert 'quota_stop:' + parallel['operation'] not in open_replacement(current)['effects']


def test_parallel_quota_storage_failure_prevents_stop_and_retries_same_attempt(environment, monkeypatch):
    """Only a committed quota reservation permits Stop; recovery retains the same native attempt."""
    from pb8_loop_storage import LoopStorageError
    service, row, _ = reserved_successor(environment)
    loops, _, _, backend = environment
    row = parallel_attempt(environment, row)
    row = loops.update(OWNER, row['id'], lambda current: (
        current['settings'].update(run_limit_mode='hours', run_hours=1),
        open_replacement(current).update(retry_at=time.time() + 600)))
    write, stop, stopped = loops.write, backend.stop, []
    def unavailable(current):
        """Fail exactly the pre-Stop archive write, leaving its previous root intact."""
        if any(job.get('limit_reached') for job in current['jobs']):
            raise LoopStorageError('fixture quota reservation storage failure')
        return write(current)
    def observed_stop(current, job):
        """The effect is issued only after its archive reservation is readable."""
        committed = loops.read(OWNER, current['id'])
        assert 'quota_stop:' + job['operation'] in open_replacement(committed)['effects']
        stopped.append(job['operation'])
        stop(current, job)
    monkeypatch.setattr(loops, 'write', unavailable)
    monkeypatch.setattr(backend, 'stop', observed_stop)
    with pytest.raises(LoopStorageError, match='quota reservation'):
        service.step(row)
    assert stopped == [] and loops.read(OWNER, row['id']) == row
    monkeypatch.setattr(loops, 'write', write)
    current = service.step(loops.read(OWNER, row['id']))
    assert stopped == ['7' * 32]
    assert next(job for job in current['jobs'] if job['operation'] == '7' * 32)['status'] == 'cancelled'


@pytest.mark.parametrize('native_status', ['running', 'completed', 'failed'])
def test_parallel_monitor_retains_real_completion_and_errors_without_quota_stop(environment, monkeypatch, native_status):
    """Waiting for replacement neither loses real results nor relabels another attempt as infrastructure."""
    service, row, _ = reserved_successor(environment)
    loops, queue, _, backend = environment
    row = parallel_attempt(environment, row)
    row = loops.update(OWNER, row['id'], lambda current: (
        current['settings'].update(run_limit_mode='hours', run_hours=4),
        open_replacement(current).update(retry_at=time.time() + 600)))
    queue.store.update('6' * 32, status=native_status, error='real parallel failure' if native_status == 'failed' else None)
    monkeypatch.setattr(backend, 'stop', lambda *_: pytest.fail('No Stop below quota or after native completion'))
    current = service.step(row)
    parallel = next(job for job in current['jobs'] if job['operation'] == '7' * 32)
    assert parallel['status'] == native_status and parallel['started']
    assert not parallel.get('limit_reached') and not parallel.get('gpu_replacement_id')
    assert bool(parallel.get('ended_at')) == (native_status != 'running')
    assert parallel.get('error') == ('real parallel failure' if native_status == 'failed' else None)
    assert current['status'] == 'replacing_gpu' and current['history'] == row['history']


@pytest.mark.parametrize('action', ['pause', 'stop', 'deadline', 'storage', 'data'])
def test_reserved_guard_cannot_create_after_new_control_or_changed_data(environment, monkeypatch, action):
    """Recheck the exact archived authorization immediately before the provider request."""
    from vast_job_runner import guard_step
    from pb8_loop_backend import LoopBackend
    from pb8_loop_storage import LoopStorageError
    service, row, intent = authorized_lease(environment)
    loops, queue, _, backend = environment
    identifier = intent['id']
    monkeypatch.setattr(LoopBackend, 'fingerprint', lambda *_: dict(row['fingerprint'], pb8='changed') if action == 'data' else row['fingerprint'])
    controller = LoopController(queue.root.parent.parent, store=loops, backend=backend)
    if action in {'pause', 'stop'}:
        controller.action(OWNER, row['id'], action)
    elif action == 'storage':
        loops.pause_storage(OWNER, row['id'], LoopStorageError('disk full'))
    elif action == 'deadline':
        loops.update(OWNER, row['id'], lambda current: current.update(deadline=time.time() - 1))
    provider = SimpleNamespace(request=lambda *_: pytest.fail('No paid Create after invalidation'))
    if action == 'data':
        assert not guard_step(queue.store, identifier, provider, intent, '')
        assert 'training data changed' in queue.store.read(identifier)['authorization_wait_reason']
    else:
        with pytest.raises(VastError, match='paused|stopped|expired|stale'):
            guard_step(queue.store, identifier, provider, intent, '')
    assert not (queue.store.directory(identifier) / 'attempt.json').exists()


@pytest.mark.parametrize('boundary', ['fingerprint', 'provider'])
@pytest.mark.parametrize('action', ['pause', 'stop', 'storage'])
def test_replacement_create_releases_transaction_locks_for_controls(environment, monkeypatch, boundary, action):
    """Input scans and in-flight provider calls allow independent controls to commit."""
    import threading
    from file_lock import advisory_file_lock
    from pb8_loop_backend import LoopBackend
    from pb8_loop_storage import LoopStorageError
    from vast_job_runner import guard_step
    _, row, intent = authorized_lease(environment)
    loops, queue, _, backend = environment
    worker_id = intent['id']
    controller = LoopController(loops.root.parent.parent, store=loops, backend=backend)
    threads, errors, calls = [], [], []
    finished = threading.Event()

    def control():
        """Exercise the actual Loop-before-Queue ordering in another thread."""
        try:
            with advisory_file_lock(loops.path(OWNER, row['id'])), advisory_file_lock(queue.root / '.queue-lock'):
                if action == 'storage':
                    loops.pause_storage(OWNER, row['id'], LoopStorageError('fixture disk full'))
                else:
                    controller.action(OWNER, row['id'], action)
        except Exception as exc:
            errors.append(exc)
        finally:
            finished.set()

    def competing_control():
        """Require persisted control while this synthetic slow boundary is still open."""
        thread = threading.Thread(target=control, name='fixture-create-control', daemon=True)
        threads.append(thread)
        thread.start()
        assert finished.wait(3), 'Create retained a Loop or Queue transaction lock'
        assert not errors

    def fingerprint(*_):
        """No real data scan; the control must finish before verification returns."""
        if boundary == 'fingerprint':
            competing_control()
        return row['fingerprint']

    class Provider:
        """An already claimed Create can lose its response and still require cleanup."""
        def request(self, method, path, *_):
            """The durable marker predates the request, which holds no transaction locks."""
            calls.append((method, path))
            assert queue.store.read(worker_id, 'attempt.json')['attempted_at']
            competing_control()
            raise VastError('Provider response timed out')

        def instances(self, **kwargs):
            """Later recovery observes absence without sending another Create."""
            assert kwargs == {'fresh': True}
            return []

    monkeypatch.setattr(LoopBackend, 'fingerprint', fingerprint)
    try:
        with pytest.raises(VastError, match='paused|stopped|expired|stale|timed out'):
            guard_step(queue.store, worker_id, Provider(), intent, '')
    finally:
        for thread in threads:
            thread.join(3)
            assert not thread.is_alive()
    assert not errors and finished.is_set()
    assert calls == ([] if boundary == 'fingerprint' else [('PUT', '/asks/999/')])
    assert loops.read(OWNER, row['id'])['status'] == ('stopping' if action == 'stop' else 'paused')
    marker = queue.store.directory(worker_id) / 'attempt.json'
    assert marker.exists() == (boundary == 'provider')
    if marker.exists():
        assert not guard_step(JobStore(queue.store.root), worker_id, Provider(), intent, '')
        assert calls == [('PUT', '/asks/999/')]
        assert queue.store.read(worker_id)['rental_state'] != 'deletion_verified'


@pytest.mark.parametrize('changed', ['fingerprint', 'initial_config', 'successor', 'input_digest'])
def test_replacement_create_rejects_input_changed_during_unlocked_scan(environment, monkeypatch, changed):
    """A scan receipt cannot authorize a different archived dataset or successor input."""
    from pb8_loop_backend import LoopBackend
    from vast_job_runner import guard_step
    _, row, intent = authorized_lease(environment)
    loops, queue, _, _ = environment

    def fingerprint(*_):
        """Change only isolated persisted input while returning the original scan result."""
        def mutate(current):
            operation = open_replacement(current)
            if changed == 'fingerprint':
                current['fingerprint']['pb8'] = 'changed'
            elif changed == 'initial_config':
                current['initial_config']['optimize']['seed'] = 654321
            elif changed == 'input_digest':
                operation['input_digest'] = 'f' * 64
            else:
                successor = next(job for job in current['jobs'] if job['operation'] == operation['successor'])
                successor['config']['optimize']['seed'] = 654321
        loops.update(OWNER, row['id'], mutate)
        return row['fingerprint']

    monkeypatch.setattr(LoopBackend, 'fingerprint', fingerprint)
    provider = SimpleNamespace(request=lambda *_: pytest.fail('Changed input must never reach Create'))
    with pytest.raises(VastError, match='input changed during verification'):
        guard_step(queue.store, intent['id'], provider, intent, '')
    assert 'input changed during verification' in queue.store.read(intent['id'])['authorization_wait_reason']
    assert not (queue.store.directory(intent['id']) / 'attempt.json').exists()


def test_storage_failure_at_rent_reservation_prevents_any_paid_effect(environment, monkeypatch):
    """No Stop/Delete/Create follows a failed durable pre-effect reservation."""
    from pb8_loop_storage import LoopStorageError
    service, row, _ = reserved_successor(environment)
    loops, queue, _, _ = environment
    monkeypatch.setattr('vast_pool.select_offer', lambda *_: {'id': 999, 'machine_id': 202})
    monkeypatch.setattr(CloudQueue, 'start', lambda *_args, **_kwargs: pytest.fail('Reservation was not durable'))
    write = loops.write
    def fail_reservation(current):
        """The prior valid archive root remains available when the new reservation cannot commit."""
        if open_replacement(current).get('reservation'):
            raise LoopStorageError('disk full during rent reservation')
        return write(current)
    monkeypatch.setattr(loops, 'write', fail_reservation)
    with pytest.raises(LoopStorageError):
        service.step(row)
    restored = LoopStore(loops.root).read(OWNER, row['id'])
    assert not open_replacement(restored).get('reservation')
    assert len(queue.store.list()) == 3


def test_paused_replacement_is_not_handover_source_or_rental_extension(environment):
    """Visible Pause cannot bypass the open operation's frozen rental authorization."""
    from pb8_loop_handover import source_worker
    from pb8_loop_rental_controls import rental_snapshot
    service, row, _ = begin(environment)
    loops, queue, _, backend = environment
    queue.store.update(WORKER, deadline_protocol=2)
    row = LoopController(queue.root.parent.parent, store=loops, backend=backend).action(OWNER, row['id'], 'pause')
    with pytest.raises(VastError, match='open GPU replacement'):
        source_worker(loops, queue, OWNER, row['id'])
    snapshot = rental_snapshot(queue, row)
    assert not snapshot['can_extend'] and not snapshot['can_edit_budget']


@pytest.mark.parametrize('status', ['cancelled', 'failed', 'complete'])
def test_infrastructure_classification_requires_authorized_stop_and_real_cancellation(environment, status):
    """Neither arbitrary cancellation nor real optimizer failure/success becomes a GPU interruption."""
    loops, queue, row, _ = environment
    job = copy.deepcopy(row['jobs'][0])
    job.update(status=status, error='actual backend evidence')
    assert optimizer_execution_evidence(job, row['settings'], row)['completion_reason'] is None
    service, row, _ = begin(environment)
    job = dict(row['jobs'][0], status=status)
    assert optimizer_execution_evidence(job, row['settings'], row)['completion_reason'] is None
    row = service.effect(row, 'stop')
    evidence = optimizer_execution_evidence(job, row['settings'], row)
    assert evidence['completion_reason'] == ('gpu_replaced' if status == 'cancelled' else None)
    assert evidence['backend_status'] == status


@pytest.mark.parametrize('max_runs', [2, 4])
def test_verified_failed_replacement_retries_without_dead_end_or_unlimited_allowance(environment, max_runs):
    """Confirmed failed capacity can recover, while every disposed attempt and rental cost remains charged."""
    service, row, intent = authorized_lease(environment)
    loops, queue, _, _ = environment
    first_successor = row['jobs'][-1]['operation']
    row = service.step(row)
    assert open_replacement(row)['step'] == 'resume'
    queue.store.update(intent['id'], rental_state='deletion_verified', absent_checks=2,
                       provider_checked_at=time.time())
    row = service.step(row)
    assert open_replacement(row)['step'] == 'retry_collection'
    row = service.step(row)
    assert row['jobs'][-1]['status'] == 'cancelled' and row['jobs'][-1]['attempt_charged']
    assert open_replacement(row)['chain'] == [WORKER, intent['id']]
    assert open_replacement(row)['excluded_machine_ids'] == [101, 202]
    assert optimizer_execution_evidence(row['jobs'][-1], row['settings'], row)['completion_reason'] == 'gpu_replaced'
    row = loops.update(OWNER, row['id'], lambda current: current['settings'].update(max_runs=max_runs))
    row = service.step(row)
    assert row['status'] == 'replacing_gpu'
    if max_runs == 2:
        assert len(row['jobs']) == 2 and 'attempt allowance' in row['reason']
    else:
        assert len(row['jobs']) == 3 and attempts_used(row) == 3
        successor = row['jobs'][-1]
        assert successor['gpu_predecessor'] == first_successor
        for key in ('config', 'overrides', 'optimizer_start', 'optimizer_archive', 'round'):
            assert successor[key] == row['jobs'][0][key]
        assert open_replacement(row)['worker_id'] != intent['id']


def test_browser_review_helper_suppresses_duplicate_and_stale_execution():
    """Exercise real JavaScript with delayed reviews, navigation and an explicit cancelled review."""
    import subprocess
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    script = r'''
const vm=browserScope,fs=testSources,assert=browserAssert;
let calls=[],release,current=true,accept=true,reviews=0;
const context={window:{PBGuiDialogs:{confirm:async()=>{reviews++;return accept;}}},
 Set,AbortController,setTimeout,clearTimeout,Date,Number,encodeURIComponent,
 fetch:async(url,options)=>{calls.push([url,JSON.parse(options.body)]);
  if(url.endsWith('replace-preview')){await new Promise(done=>release=done);return {ok:true,json:async()=>({
   snapshot:{generation:3},snapshot_digest:'f'.repeat(64),operation_id:'e'.repeat(32),
   excluded_machine_ids:[101],attempts_used:1,max_runs:4,remaining_budget_usd:1,deadline:2000000000})};}
  return {ok:true,json:async()=>({status:'replacing_gpu'})};}};
vm.runInNewContext(fs.readFileSync('frontend/js/loop_gpu_replacement.js','utf8'),context);
const options={base:'/mounted/api/optimize-v8/loops',loop_id:'a'.repeat(32),worker_id:'b'.repeat(32),isCurrent:()=>current};
(async()=>{
 let first=context.window.PBGuiLoopGPUReplacement.run(options);
 await context.window.PBGuiLoopGPUReplacement.run(options);assert.equal(calls.length,1);
 current=false;release();await first;assert.equal(reviews,0);assert.equal(calls.length,1);
 current=true;accept=false;first=context.window.PBGuiLoopGPUReplacement.run(options);release();await first;
 assert.equal(reviews,1);assert.equal(calls.length,2);
 accept=true;first=context.window.PBGuiLoopGPUReplacement.run(options);release();await first;
 assert.equal(calls.length,4);assert.ok(calls[3][0].startsWith(options.base));
 assert.equal(calls[3][1].expected_generation,3);assert.equal(calls[3][1].authorization,true);
 assert.equal(calls[3][1].operation_id,'e'.repeat(32));
})().catch(error=>{console.error(error);testResult.exitCode=1;});
'''
    result = run_javascript(['-e', script], cwd=root, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr


def test_stop_collection_has_shared_deadline_and_retains_available_artifacts(environment, monkeypatch):
    """Persist Stop/wait/collection bounds so process restarts cannot extend collection indefinitely."""
    from vast_job_runner import stop_and_collect
    _, queue, _, _ = environment
    clock = [1000.0]
    monkeypatch.setattr('vast_job_runner.time.time', lambda: clock[0])
    operations, collections = [], []
    artifact = queue.store.directory(NATIVE) / 'partial.tar.gz'
    artifact.write_bytes(b'previous verified snapshot')
    class Connection:
        """A stuck remote optimizer consumes only the supplied bounded time."""
        def operation(self, action, timeout):
            """Stop consumes thirty seconds; no progress is invented."""
            operations.append((action, timeout))
            clock[0] += timeout
            return {}
        def collect(self, final, timeout):
            """Partial metadata/download cannot obtain a second collection allowance."""
            assert not final
            collections.append(timeout)
            clock[0] += timeout
            raise VastError('Transfer deadline reached')
    intent = {'deadline': 2000}
    assert stop_and_collect(Connection(), queue.store, NATIVE, intent, lease_id=WORKER)
    assert operations == [('stop', 30), ('status', 30)]
    assert collections == [180]
    assert clock[0] == 1240
    state = queue.store.read(NATIVE)
    assert state['status'] == 'cancelled' and state['collection_incomplete']
    assert artifact.read_bytes() == b'previous verified snapshot'
    assert stop_and_collect(Connection(), queue.store, NATIVE, intent, lease_id=WORKER)
    assert operations == [('stop', 30), ('status', 30)] and collections == [180]
    assert not queue.store.read(WORKER, 'control.json')['cleanup']


@pytest.mark.parametrize('outcome', ['partial', 'cancelled', 'completed', 'failed'])
@pytest.mark.parametrize('publication_timeout', [False, True])
def test_stop_publishes_last_verified_results_without_hiding_execution_failure(environment, monkeypatch, outcome, publication_timeout):
    """The final Stop snapshot reaches native candidates before the runner exits."""
    import msgpack
    from pathlib import Path
    from vast_job_runner import stop_and_collect
    from vast_jobs import write_json
    _, queue, _, _ = environment
    directory = queue.store.directory(NATIVE)
    results_root = directory / 'published/optimize_results'
    write_json(directory / 'intent.json', dict(id=NATIVE, image=IMAGE, pb8_revision=REVISION,
        results_root=str(results_root), source_config_sha256='f' * 64))
    (directory / 'input').mkdir()
    write_json(directory / 'input/manifest.json', {})
    clock, collections = [1000.0], []
    monkeypatch.setattr('vast_job_runner.time.time', lambda: clock[0])
    if publication_timeout:
        from vast_transfer import import_results
        def expired_publication(*args, **kwargs):
            """The existing collection deadline also applies to local result publication."""
            clock[0] += 181
            return import_results(*args, **kwargs)
        monkeypatch.setattr('vast_job_runner.import_results', expired_publication)
    class Connection:
        """Publish isolated verified artifacts without contacting any worker."""
        def operation(self, action, timeout):
            """A running partial snapshot exhausts only the existing Stop wait."""
            clock[0] += timeout
            return {'finished': outcome != 'partial'}
        def collect(self, final, timeout):
            """Model the verified output contract of WorkerConnection.collect."""
            collections.append(final)
            assert final is (outcome != 'partial')
            root = directory / ('final-results' if final else 'partial-results')
            source = root / 'optimize_results/fixture'
            source.mkdir(parents=True)
            (source / 'all_results.bin').write_bytes(msgpack.packb({'metrics': {'gain': 1}}))
            if final:
                write_json(root / 'finished.json', dict(cancelled=outcome == 'cancelled',
                    exit_code=1 if outcome == 'failed' else 130 if outcome == 'cancelled' else 0))
            queue.store.update(NATIVE, final_collected=final, last_backup_at=clock[0])
    assert stop_and_collect(Connection(), queue.store, NATIVE, {'deadline':2000}, lease_id=WORKER)
    current = queue.store.read(NATIVE)
    assert current['status'] == ('cancelled' if outcome == 'partial' else outcome)
    assert current['stop_reason'] == 'requested'
    if publication_timeout:
        assert not current.get('result_path') and current['collection_incomplete']
        assert current['collection_error']
        snapshot = directory / ('partial-results' if outcome == 'partial' else 'final-results')
        assert (snapshot / 'optimize_results/fixture/all_results.bin').is_file()
    else:
        assert current['evaluations'] == 1 and Path(current['result_path']).is_dir()
        assert current['result_partial'] is (outcome == 'partial')
        assert ('exited with code 1' in (current.get('error') or '')) is (outcome == 'failed')
    # A recovered supervisor must neither download again nor duplicate the import.
    assert stop_and_collect(Connection(), queue.store, NATIVE, {'deadline':2000}, lease_id=WORKER)
    assert len(collections) == 1
    assert len(list(results_root.glob('fixture_vast_*'))) == (0 if publication_timeout else 1)
    if publication_timeout and outcome != 'partial':
        from vast_job_runner import finalize_collected_results
        # A repeated API/supervisor retry failure must retain the Stop-origin
        # execution evidence too; only an ordinary import uses generic failed.
        def retry_error(*args, **kwargs):
            """Retry has a fresh allowance but its local storage may still fail."""
            raise VastError('Isolated local publication error')
        monkeypatch.setattr('vast_job_runner.import_results', retry_error)
        with pytest.raises(VastError):
            finalize_collected_results(queue.store, NATIVE)
        retry_failed = queue.store.read(NATIVE)
        assert retry_failed['status'] == outcome and retry_failed['stop_finalization_pending']
        monkeypatch.setattr('vast_job_runner.import_results', import_results)
        for _ in range(2):
            recovered = finalize_collected_results(queue.store, NATIVE)
            assert recovered['status'] == outcome
            assert not recovered['collection_incomplete'] and recovered['collection_error'] is None
            assert not recovered['stop_finalization_pending']
            assert ('exited with code 1' in (recovered.get('error') or '')) is (outcome == 'failed')
        assert len(list(results_root.glob('fixture_vast_*'))) == 1
        assert len(collections) == 1


def test_upload_children_and_executor_are_joined_after_control_stop(tmp_path, monkeypatch):
    """Both real local SSH stand-ins terminate and both transfer threads join on Stop."""
    import subprocess
    import sys
    import threading
    from vast_transfer import WorkerConnection
    connection = object.__new__(WorkerConnection)
    connection.identifier, connection.lease_id = NATIVE, WORKER
    connection.directory = connection.identity_directory = tmp_path
    connection.remote_root = '/work/pbgui/jobs/' + NATIVE
    connection.host, connection.port = '127.0.0.1', 22
    controls, children = {'stop': False}, []
    connection.store = SimpleNamespace(read=lambda *_: dict(controls), update=lambda *_args, **_kwargs: None)
    original = subprocess.Popen
    def spawn(_args, **kwargs):
        """Use only isolated local children; no SSH or network access occurs."""
        child = original([sys.executable, '-c', 'import sys,time;sys.stdin.buffer.read();time.sleep(30)'], **kwargs)
        children.append(child)
        if len(children) == 2:
            controls['stop'] = True
        return child
    def command(value, **kwargs):
        """The inventory is empty; both actual transfer owners must be cancelled."""
        if kwargs.get('stdout') is None:
            return b'[]'
        return WorkerConnection.command(connection, value, **kwargs)
    monkeypatch.setattr('vast_transfer.subprocess.Popen', spawn)
    connection.command = command
    archive = tmp_path / 'upload.tar.gz'
    archive.write_bytes(b'a' * 512 + b'b' * 512)
    started = time.monotonic()
    with pytest.raises(VastError, match='cancelled'):
        connection.upload_archive(archive, timeout=15, chunk_bytes=512)
    assert time.monotonic() - started < 8
    assert len(children) == 2 and all(child.poll() is not None for child in children)
    assert all(child.stdout.closed and child.stderr.closed for child in children)
    assert not any(thread.name.startswith('vast-upload') for thread in threading.enumerate())


def test_owned_process_group_escalates_and_reaps_uncooperative_child():
    """An isolated child that ignores TERM must be killed after the five-second grace."""
    import subprocess
    import sys
    from vast_transfer import reap_process_group
    child = subprocess.Popen([sys.executable, '-c',
        'import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);print("ready",flush=True);time.sleep(30)'],
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    try:
        assert child.stdout.readline() == b'ready\n'
        started = time.monotonic()
        reap_process_group(child)
        assert 4.8 <= time.monotonic() - started < 8
        assert child.returncode == -9 and child.stdout.closed and child.stderr.closed
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()


@pytest.mark.parametrize('reconstruct', [False, True])
@pytest.mark.parametrize('profile_kind', ['none', 'rental', 'job', 'both'])
def test_real_queue_and_job_start_use_bound_remaining_authorization_without_global_auto_rent(environment, monkeypatch, reconstruct, profile_kind):
    """The complete local start path accepts only its reserved worker and reduced limits beside another rental."""
    from vast_jobs import write_json
    loops, queue, initial, _ = environment
    loops.update(OWNER, initial['id'], lambda current: current['settings']['vast'].update(max_rentals=2))
    preferences = queue.read()['gpu_preferences']
    preferences['max_rentals'] = 2
    queue.update(gpu_preferences=preferences)
    rental_profile = dict(population_size=8192, batch_size=1024, max_dispatch_candidate_bars=123456789)
    job_profile = dict(population_size=4096, batch_size=512, max_dispatch_candidate_bars=987654321)
    old_intent = queue.store.read(WORKER, 'intent.json')
    old_intent.update(rental_gpu_profile=rental_profile if profile_kind in {'rental', 'both'} else None,
        rental_job_gpu_profiles={NATIVE: job_profile, '8' * 32: rental_profile}
            if profile_kind in {'job', 'both'} else {})
    write_json(queue.store.directory(WORKER) / 'intent.json', old_intent)
    service, row, intent = authorized_lease(environment)
    worker_id = intent['id']
    successor_id = row['jobs'][-1]['backend']['id']
    if not reconstruct:
        # Exercise the first start branch, not the persisted-reservation retry.
        import shutil
        shutil.rmtree(queue.store.directory(worker_id))
        row = service.change(row, lambda current, op: op.update(reservation=None))
        monkeypatch.setattr('vast_pool.select_offer', lambda *_: intent['offer'])
    intent.update(bundle_bytes=0, job_count=1)
    if reconstruct:
        write_json(queue.store.directory(worker_id) / 'intent.json', intent)
        queue.store.update(worker_id, rental_state='none', status='ready')
    queue.store.publish(dict(id='9' * 32, kind='worker', status='running', rental_state='active',
        loop_id='8' * 32, loop_owner=OWNER, budget_usd=1))
    monkeypatch.setattr('vast_queue.preflight_gpu_snapshots', lambda store, jobs, **_: jobs)
    monkeypatch.setattr('vast_queue.worker_compatible_jobs', lambda store, jobs, **_: jobs)
    monkeypatch.setattr('vast_queue.preflight_local_metadata', lambda *_: None)
    monkeypatch.setattr('vast_jobs.services_available', lambda: True)
    monkeypatch.setattr('vast_image.require_public_image', lambda *_: None)
    monkeypatch.setattr('vast_jobs.VastCredentialStore', lambda *_: SimpleNamespace(secrets=lambda: {'api_key':'fixture', 'generation':1}))
    monkeypatch.setattr('vast_jobs.VastClient', lambda *_: SimpleNamespace(account=lambda: {'balance_usd':100}))
    start = CloudQueue.start
    def checked_start(scoped, *args, **kwargs):
        """A valid profile cannot be substituted for either frozen authorization field."""
        for changed in ('rental_gpu_profile', 'rental_job_gpu_profiles'):
            bad = dict(kwargs)
            altered = dict(population_size=16384, batch_size=256, max_dispatch_candidate_bars=111111111)
            bad[changed] = altered if changed == 'rental_gpu_profile' else {successor_id: altered}
            with pytest.raises(VastError, match='profiles differ'):
                start(scoped, *args, **bad)
        return start(scoped, *args, **kwargs)
    monkeypatch.setattr(CloudQueue, 'start', checked_start)
    current = service.step(row)
    assert current['status'] == 'replacing_gpu' and open_replacement(current)['step'] == 'resume'
    assert queue.store.read(worker_id)['rental_state'] == 'creation_pending'
    stored = queue.store.read(worker_id, 'intent.json')
    assert stored['gpu_replacement'] == open_replacement(current)['reservation']
    assert stored['rental_gpu_profile'] == old_intent['rental_gpu_profile']
    assert stored['rental_job_gpu_profiles'] == (
        {successor_id: job_profile} if profile_kind in {'job', 'both'} else {})
    assert NATIVE not in stored['rental_job_gpu_profiles']
    assert stored['budget_usd'] < 3 and stored['deadline'] <= initial['deadline']
    assert queue.store.read('9' * 32)['rental_state'] == 'active'
    assert not queue.read().get('pool_enabled') and not queue.read()['gpu_preferences']['auto_rent']


def test_replacement_review_rejects_changed_manual_gpu_profiles(environment):
    """Changing the selected rental profile invalidates a previously reviewed digest."""
    from vast_jobs import write_json
    loops, queue, row, backend = environment
    data = review(queue, row, WORKER)
    intent = queue.store.read(WORKER, 'intent.json')
    intent['rental_gpu_profile'] = dict(population_size=8192, batch_size=1024, max_dispatch_candidate_bars=123456789)
    write_json(queue.store.directory(WORKER) / 'intent.json', intent)
    with pytest.raises(VastError, match='changed since review'):
        GPUReplacement(loops, queue, backend).begin(OWNER, row['id'], WORKER, row['control_generation'],
            data['snapshot_digest'], OPERATION)
    assert loops.read(OWNER, row['id'])['status'] == row['status']
    assert not queue.store.read(WORKER, 'control.json')['stop']


@pytest.mark.parametrize('max_runs', [2, 4])
def test_interrupted_preparation_after_api_restart_cannot_leave_an_unrentable_successor(environment, monkeypatch, max_runs):
    """A failed native preparation is retained and charged before a bounded preserved-input retry."""
    service, row, _ = advance_to_capacity(environment)
    loops, queue, _, backend = environment
    row = service.step(row)
    original = copy.deepcopy(row['jobs'][-1])
    prepare = backend.prepare
    def interrupted(current, job):
        """Represent LoopBackend recovering an interrupted native preparing record."""
        native = prepare(current, job)
        queue.store.update(native['id'], status='failed', error='Input preparation interrupted by process shutdown')
        return native
    monkeypatch.setattr(backend, 'prepare', interrupted)
    row = service.step(row)
    assert row['status'] == 'replacing_gpu' and open_replacement(row)['step'] == 'capacity'
    assert row['jobs'][-1]['status'] == 'failed' and row['jobs'][-1]['attempt_charged']
    assert attempts_used(row) == 2 and len(queue.store.list()) == 3
    row = loops.update(OWNER, row['id'], lambda current: (
        current['settings'].update(max_runs=max_runs), open_replacement(current).update(retry_at=0)))
    row = service.step(row)
    if max_runs == 2:
        assert len(row['jobs']) == 2 and 'attempt allowance' in row['reason']
    else:
        assert attempts_used(row) == 3 and row['jobs'][-1]['gpu_predecessor'] == original['operation']
        for key in ('config', 'overrides', 'optimizer_start', 'optimizer_archive', 'round'):
            assert row['jobs'][-1][key] == original[key]
