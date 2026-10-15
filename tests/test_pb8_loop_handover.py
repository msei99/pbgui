"""Offline GPU handover tests using isolated Loop/worker records only."""
import copy
import fcntl
import os
import time

import pytest

from pb8_loop_handover import advance, begin
from pb8_loop_store import LoopStore
from secure_files import ensure_private_directory
from vast_jobs import JobStore, write_json, IMAGE, REVISION
from vast_provider import VastError
from vast_queue import CloudQueue, worker_step
from vast_direction_fixtures import direction_config

OWNER = 'f' * 32
WORKER = 'a' * 32


@pytest.fixture
def environment(tmp_path, monkeypatch):
    """Create real atomic stores with no network, bot processes or runtime data."""
    loops = LoopStore(tmp_path / 'data/loop_optimizer')
    queue = CloudQueue(JobStore(tmp_path / 'vast'))
    config = direction_config()
    settings = {'hours': 12, 'execution': 'vast', 'vast': {'budget': 2, 'hours': 12}, 'goals': {}}
    source = loops.create(OWNER, settings, config, {}, {}, [], {})
    target = loops.create(OWNER, settings, config, {}, {}, [], {}, queued=True)
    directory = ensure_private_directory(queue.store.root / 'jobs' / WORKER)
    write_json(directory / 'state.json', {'id': WORKER, 'kind': 'worker', 'status': 'running',
        'rental_state': 'active', 'loop_id': source['id'], 'loop_owner': OWNER,
        'workers': 4, 'deadline': time.time() + 7200, 'budget_usd': 2, 'price_hour_usd': .1,
        'transfer_reserved_used': .12, 'idle_seconds': 1, 'idle_since': time.time() - 1000})
    write_json(directory / 'control.json', {'stop': False, 'cleanup': False})
    write_json(directory / 'intent.json', {'image': IMAGE, 'pb8_revision': REVISION,
        'budget_usd': 2, 'hours': 12, 'transfer_reserve_usd': .3,
        'offer': {'download_gb_usd': 0, 'upload_gb_usd': 0}})
    monkeypatch.setattr('vast_queue.PROJECT', tmp_path)
    return loops, queue, source, target


def collected(loops, source, target):
    """Represent fully stopped native jobs and an authorized replacement start."""
    loops.update(OWNER, source['id'], lambda r: r.update(status='stopped', cleanup_done=True))
    return loops.update(OWNER, target['id'], lambda r: r.update(status='running', deadline=time.time() + 43200))


def test_handover_preserves_rental_limits_and_survives_restart(environment):
    """Old-job collection holds the same GPU; transfer never renews paid allowances."""
    loops, queue, source, target = environment
    before = copy.deepcopy(queue.store.read(WORKER))
    intent = queue.store.read(WORKER, 'intent.json')
    handover = begin(loops, queue, OWNER, source['id'], target['id'], expected_worker=WORKER,
                     expected_generation=source['control_generation'])
    assert loops.read(OWNER, source['id'])['status'] == 'stopping'
    assert begin(loops, queue, OWNER, source['id'], target['id']) == handover
    # Idle timeout has already elapsed, but handover blocks both cleanup and dispatch.
    assert worker_step(queue, WORKER) is None
    assert not queue.store.read(WORKER, 'control.json')['cleanup']
    assert not advance(loops, queue, loops.read(OWNER, target['id']))
    target = collected(loops, source, target)
    # Reconstruct both stores as an API/supervisor restart would.
    loops = LoopStore(loops.root)
    queue = CloudQueue(JobStore(queue.root))
    assert advance(loops, queue, target)
    worker = queue.store.read(WORKER)
    assert worker['loop_id'] == target['id']
    for key in ('deadline', 'budget_usd', 'price_hour_usd', 'transfer_reserved_used'):
        assert worker[key] == before[key]
    assert queue.store.read(WORKER, 'intent.json') == intent
    assert not queue.store.read(WORKER, 'control.json')['cleanup']
    assert loops.read(OWNER, target['id'])['deadline'] == before['deadline'] - 240
    assert advance(loops, queue, loops.read(OWNER, target['id']))
    # Normal production dispatch must now accept only replacement jobs.
    for job_id, loop_id in [('b' * 32, target['id']), ('c' * 32, source['id'])]:
        directory = ensure_private_directory(queue.store.root / 'jobs' / job_id)
        write_json(directory / 'state.json', {'id': job_id, 'kind': 'job', 'status': 'ready',
            'rental_state': 'none', 'loop_id': loop_id, 'loop_owner': OWNER,
            'workers': 4, 'input_bytes': 100, 'created_at': time.time()})
        ensure_private_directory(directory / 'input')
        write_json(directory / 'input/optimize.json', direction_config())
    assert worker_step(queue, WORKER) == 'b' * 32
    assert queue.store.read('b' * 32)['lease_id'] == WORKER
    assert queue.store.read('c' * 32)['status'] == 'ready'


@pytest.mark.parametrize('blocked', ['active_job', 'native_job', 'observer', 'collection'])
def test_transfer_waits_for_every_source_job(environment, blocked):
    """A completed status alone cannot transfer a GPU with outstanding work."""
    loops, queue, source, target = environment
    begin(loops, queue, OWNER, source['id'], target['id'])
    target = collected(loops, source, target)
    if blocked == 'active_job':
        queue.store.update(WORKER, active_job='b' * 32)
    elif blocked == 'native_job':
        directory = ensure_private_directory(queue.store.root / 'jobs' / ('b' * 32))
        write_json(directory / 'state.json', {'id': 'b' * 32, 'kind': 'job', 'lease_id': WORKER, 'status': 'collecting'})
    elif blocked == 'observer':
        loops.update(OWNER, source['id'], lambda r: r.update(observer_jobs=[{'status': 'running'}]))
    else:
        loops.update(OWNER, source['id'], lambda r: r.update(cleanup_done=False))
    assert not advance(loops, queue, target)
    assert queue.store.read(WORKER)['loop_id'] == source['id']


@pytest.mark.parametrize('invalid', ['owner', 'budget', 'offline', 'worker', 'generation', 'cleanup', 'deadline', 'same'])
def test_invalid_handover_does_not_stop_or_reserve(environment, invalid):
    """Reject unsafe/stale requests before mutating either Loop or worker."""
    loops, queue, source, target = environment
    kwargs = {}
    if invalid == 'owner':
        queue.store.update(WORKER, loop_owner='e' * 32)
    elif invalid == 'budget':
        loops.update(OWNER, target['id'], lambda r: r['settings']['vast'].update(budget=4))
    elif invalid == 'offline':
        loops.update(OWNER, target['id'], lambda r: r['initial_config']['backtest'].update(offline=False))
    elif invalid == 'worker':
        kwargs['expected_worker'] = 'b' * 32
    elif invalid == 'generation':
        kwargs['expected_generation'] = 99
    elif invalid == 'cleanup':
        queue.store.control(WORKER, 'cleanup')
    elif invalid == 'deadline':
        queue.store.update(WORKER, deadline=time.time() + 250)
    elif invalid == 'same':
        target = source
    before = (loops.read(OWNER, source['id']), loops.read(OWNER, target['id']), queue.store.read(WORKER))
    with pytest.raises((VastError, FileNotFoundError)):
        begin(loops, queue, OWNER, source['id'], target['id'], **kwargs)
    assert (loops.read(OWNER, source['id']), loops.read(OWNER, target['id']), queue.store.read(WORKER)) == before


def test_handover_cannot_race_inflight_controller(environment):
    """Do not interrupt a controller's native launch or acknowledged AI decision."""
    loops, queue, source, target = environment
    fd = os.open(loops.path(OWNER, source['id']).with_suffix('.controller'), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(VastError, match='step is in progress'):
            begin(loops, queue, OWNER, source['id'], target['id'])
    finally:
        os.close(fd)
    assert loops.read(OWNER, source['id'])['status'] == 'running'


def test_replay_recovers_interrupted_worker_reservation(environment, monkeypatch):
    """A crash between worker and target writes is recoverable without a new rental."""
    loops, queue, source, target = environment
    original = loops.update
    def interrupted(owner, identifier, fn):
        """Fail the first target persistence after the write-ahead worker reservation."""
        if identifier == target['id']:
            raise OSError('simulated crash')
        return original(owner, identifier, fn)
    monkeypatch.setattr(loops, 'update', interrupted)
    with pytest.raises(OSError):
        begin(loops, queue, OWNER, source['id'], target['id'])
    assert queue.store.read(WORKER)['loop_handover']['status'] == 'collecting'
    assert worker_step(queue, WORKER) is None
    monkeypatch.setattr(loops, 'update', original)
    begin(loops, queue, OWNER, source['id'], target['id'])
    assert loops.read(OWNER, source['id'])['status'] == 'stopping'


def test_replay_recovers_interrupted_transfer(environment, monkeypatch):
    """A crash after re-binding cannot reset the deadline or transfer twice."""
    loops, queue, source, target = environment
    begin(loops, queue, OWNER, source['id'], target['id'])
    target = collected(loops, source, target)
    original = loops.update
    monkeypatch.setattr(loops, 'update', lambda *args: (_ for _ in ()).throw(OSError('simulated crash')))
    with pytest.raises(OSError):
        advance(loops, queue, target)
    assert queue.store.read(WORKER)['loop_id'] == target['id']
    monkeypatch.setattr(loops, 'update', original)
    assert advance(loops, queue, target)
    assert loops.read(OWNER, target['id'])['deadline'] == queue.store.read(WORKER)['deadline'] - 240


@pytest.mark.parametrize('ending', ['deadline', 'cleanup'])
def test_handover_never_disables_rental_shutdown(environment, ending):
    """Retention cannot defeat explicit cleanup or the existing paid deadline."""
    loops, queue, source, target = environment
    begin(loops, queue, OWNER, source['id'], target['id'])
    if ending == 'cleanup':
        queue.store.control(WORKER, 'cleanup')
    now = queue.store.read(WORKER)['deadline'] - 200 if ending == 'deadline' else time.time()
    assert worker_step(queue, WORKER, now=now) is None
    assert queue.store.read(WORKER, 'control.json')['cleanup']


def test_stopping_replacement_cancels_reserved_rental(environment):
    """Explicit replacement cancellation releases its reservation deterministically."""
    loops, queue, source, target = environment
    begin(loops, queue, OWNER, source['id'], target['id'])
    target = loops.update(OWNER, target['id'], lambda r: r.update(status='stopping'))
    assert not advance(loops, queue, target)
    assert queue.store.read(WORKER, 'control.json')['cleanup']


def test_source_cleanup_retains_reserved_worker_and_target_never_rents(environment, monkeypatch):
    """Production cleanup/rental helpers honor the durable handover reservation."""
    from pb8_loop_backend import LoopBackend
    loops, queue, source, target = environment
    monkeypatch.setattr('vast_queue.CloudQueue', lambda: queue)
    begin(loops, queue, OWNER, source['id'], target['id'])
    backend = LoopBackend(queue.root.parent, loops)
    backend.cleanup(loops.read(OWNER, source['id']))
    assert not queue.store.read(WORKER, 'control.json')['cleanup']
    def unexpected(*args, **kwargs):
        """Any rental attempt would violate this operation's authorization."""
        pytest.fail('handover must not rent a GPU')
    monkeypatch.setattr('vast_queue.CloudQueue', unexpected)
    backend._rent(loops.read(OWNER, target['id']))


def test_controller_cancels_unlaunched_source_jobs_without_destroying_gpu(environment, monkeypatch):
    """Planned source jobs cannot keep a handover waiting forever after Stop."""
    import asyncio
    from pb8_loop_backend import LoopBackend
    from pb8_loop_controller import LoopController
    loops, queue, source, target = environment
    monkeypatch.setattr('vast_queue.CloudQueue', lambda: queue)
    loops.update(OWNER, source['id'], lambda r: r.update(jobs=[{
        'operation': 'b' * 32, 'status': 'planned', 'backend': None}]))
    begin(loops, queue, OWNER, source['id'], target['id'])
    controller = LoopController(queue.root.parent, store=loops, backend=LoopBackend(queue.root.parent, loops))
    asyncio.run(controller._tick(loops.read(OWNER, source['id'])))
    source = loops.read(OWNER, source['id'])
    assert source['status'] == 'stopped' and source['cleanup_done']
    assert source['jobs'][0]['status'] == 'cancelled'
    assert not queue.store.read(WORKER, 'control.json')['cleanup']


def test_handover_route_starts_only_after_reservation_and_preflight(environment, monkeypatch):
    """The real route binds existing state and starts a guarded target, with replay safety."""
    import asyncio
    import json
    from types import SimpleNamespace
    from api import loop_optimizer_v8 as api
    loops, queue, source, target = environment
    calls = []
    async def preflight(owner, settings):
        """Provider readiness must precede reservation or source stop."""
        calls.append('preflight')
    async def start(owner, identifier, body):
        """Model the native authorization transition without starting a real job."""
        assert loops.read(owner, source['id'])['status'] == 'stopping'
        assert queue.store.read(WORKER)['loop_handover']['status'] == 'collecting'
        assert not queue.store.read(WORKER, 'control.json')['cleanup']
        calls.append('start')
        loops.update(owner, identifier, lambda r: r.update(status='running', deadline=time.time() + 43200))
    monkeypatch.setattr(api, '_controller', SimpleNamespace(store=loops, ai=SimpleNamespace(preflight=preflight)))
    monkeypatch.setattr(api, '_loop_action_owned', start)
    monkeypatch.setattr(api, 'projection', lambda row: {k: row[k] for k in ('id', 'status', 'rental_handover')})
    monkeypatch.setattr('vast_queue.CloudQueue', lambda: queue)
    body = api.LoopHandover(target_loop_id=target['id'], worker_id=WORKER,
                            source_generation=source['control_generation'], authorization=True)
    response = asyncio.run(api._handover_owned(OWNER, source['id'], body))
    assert response.status_code == 202
    assert json.loads(response.body)['rental_handover']['worker_id'] == WORKER
    asyncio.run(api._handover_owned(OWNER, source['id'], body))
    assert calls == ['preflight', 'start', 'preflight']


def test_handover_route_requires_authorization_before_stopping(environment, monkeypatch):
    """Missing consent cannot reserve or stop anything, including via the API."""
    import asyncio
    from types import SimpleNamespace
    from fastapi import HTTPException
    from api import loop_optimizer_v8 as api
    loops, queue, source, target = environment
    monkeypatch.setattr(api, '_controller', SimpleNamespace(store=loops))
    with pytest.raises(HTTPException) as error:
        asyncio.run(api._handover_owned(OWNER, source['id'], api.LoopHandover(target_loop_id=target['id'])))
    assert error.value.status_code == 422
    assert loops.read(OWNER, source['id'])['status'] == 'running'
    assert not queue.store.read(WORKER).get('loop_handover')


@pytest.mark.parametrize('changed', [False, True])
def test_native_start_compares_persisted_rental_snapshot(environment, monkeypatch, changed):
    """Matching reviewed limits start; actual changes reject before preparing jobs."""
    import asyncio
    from types import SimpleNamespace
    from fastapi import HTTPException
    from api import loop_optimizer_v8 as api
    from api.vast import RentalPreferences
    loops, queue, source, target = environment
    frozen = RentalPreferences(hours=12, budget=2).model_dump()
    settings = api.LoopStart(max_rounds=10, config_name='test', provider='chatgpt', model='test',
                             execution='vast', hours=12).model_dump(exclude={'authorization'})
    settings['vast'] = frozen
    loops.update(OWNER, target['id'], lambda r: r.update(settings=settings,
                  rental_handover={'worker_id': WORKER, 'status': 'collecting'}))
    current = dict(frozen, budget=3) if changed else frozen
    monkeypatch.setattr('vast_queue.CloudQueue', lambda: SimpleNamespace(root=queue.root, read=lambda: {'gpu_preferences': current}))
    monkeypatch.setattr('vast_credentials.VastCredentialStore', lambda root: SimpleNamespace(metadata=lambda: {'configured': True}))
    calls = []
    async def preflight(owner, settings):
        """Record validated start without invoking a real provider."""
        calls.append('preflight')
    def initial(*args):
        """Return detached fixture inputs without touching a bot or GPU."""
        calls.append('initial')
        return target['initial_config'], {}, {}, [], {}
    monkeypatch.setattr(api, '_controller', SimpleNamespace(store=loops,
        ai=SimpleNamespace(preflight=preflight), backend=SimpleNamespace(initial=initial)))
    monkeypatch.setattr(api, 'get_ai_chat_service', lambda: SimpleNamespace(get_preferences=lambda owner: {'jev_max_cost_usd': .1}))
    monkeypatch.setattr(api, 'projection', lambda row: {'status': row['status']})
    if changed:
        with pytest.raises(HTTPException, match='Vast preferences changed'):
            asyncio.run(api._loop_action_owned(OWNER, target['id'], api.LoopAction(action='start')))
        assert calls == []
        assert loops.read(OWNER, target['id'])['status'] == 'queued'
    else:
        response = asyncio.run(api._loop_action_owned(OWNER, target['id'], api.LoopAction(action='start')))
        assert response.status_code == 200
        assert calls == ['preflight', 'initial']
        assert loops.read(OWNER, target['id'])['settings']['vast'] == frozen
