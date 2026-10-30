"""Offline regressions for API responsiveness during Loop archive operations."""
import asyncio
import json
import threading

import pytest

from pb8_loop_controller import LoopController, owned_thread
from pb8_loop_observer import ObserverJobs
from tests.test_pb8_loop_optimizer import FakeBackend, OWNER, record
from tests.test_pb8_loop_observer import Backend, reference


@pytest.mark.parametrize('case,eligible', [
    ('idle',True), ('unstarted_job',True), ('unstarted_report',True), ('running_job',False),
    ('running_report',False), ('unfinished_report',False), ('missing_holdout',False),
    ('extension',False), ('handover',False), ('replacement',False), ('pending_ai',True), ('active',False),
])
def test_paused_idle_requires_no_collectable_work(case, eligible):
    """Never suppress native collection, recovery, infrastructure changes or active work."""
    row = {'status':'paused', 'jobs':[], 'observer_jobs':[]}
    if case in {'unstarted_job', 'running_job'}:
        row['jobs'] = [{'status':'queued', 'backend':{'id':'native'} if case=='running_job' else None}]
    elif case in {'unstarted_report', 'running_report'}:
        row['observer_jobs'] = [{'status':'planned', 'backend':{'id':'native'} if case=='running_report' else None}]
    elif case in {'unfinished_report', 'missing_holdout'}:
        row['observer_jobs'] = [{'status':'complete', 'report_done':case=='missing_holdout',
                                 'kind':'observer_holdout' if case=='missing_holdout' else 'observer_full_range'}]
    elif case == 'extension':
        row['rental_extension'] = {'request':'pending'}
    elif case == 'handover':
        row['rental_handover'] = {'status':'collecting'}
    elif case == 'replacement':
        row.update(gpu_replacement_id='operation', gpu_replacements={'operation':{'step':'transfer'}})
    elif case == 'pending_ai':
        row['pending_ai'] = {'stage':'evaluate'}
    elif case == 'active':
        row['status'] = 'running'
    assert LoopController._can_idle_paused(row) is eligible


def test_paused_unstarted_reports_do_not_reread_full_run(record, monkeypatch):
    """Planned jobs with no native backend cannot advance while paused and need no per-job read."""
    store, row = record
    row.update(status='paused', jobs=[], history=[], baseline=None,
               observer_jobs=[{'operation':'a'*32, 'kind':'observer_full_range', 'round':0,
                               'status':'planned', 'backend':None, 'started':False}])

    def forbidden(*_args, **_kwargs):
        """A read here would only repeat the already-known cannot-launch decision."""
        raise AssertionError('Paused planned reports copied the complete run')

    monkeypatch.setattr(store, 'read', forbidden)
    asyncio.run(ObserverJobs(store, FakeBackend(), owned_thread).tick(row))
    assert row['observer_jobs'][0]['status'] == 'planned'


@pytest.mark.parametrize('wake', ['resume', 'deadline'])
@pytest.mark.parametrize('reserved', [False, True])
def test_unchanged_paused_loop_skips_hydration_but_wakes_for_controls_and_deadline(record, tmp_path, monkeypatch, wake, reserved):
    """Skip only identical paused state; persisted actions and time limits still wake the controller."""
    import pb8_loop_controller as module
    store, row = record
    reservation={'id':'reservation', 'stage':'evaluate', 'tokens':123} if reserved else None
    row = store.update(OWNER, row['id'], lambda value: value.update(status='paused', pending_ai=reservation))
    controller = LoopController(tmp_path, store=store, backend=FakeBackend())
    controller._evidence_retention_at = float('inf')
    ticks, reads = [], []
    original_read = store.read

    def read(*args, **kwargs):
        """Count real full snapshot reads without replacing their validation."""
        reads.append(True)
        return original_read(*args, **kwargs)

    async def step(current):
        """Observe scheduling while avoiding native launch and paid/provider work."""
        ticks.append(current['status'])

    async def observers(_current):
        """This fixture has no native observer work to collect."""
        return None

    monkeypatch.setattr(store, 'read', read)
    monkeypatch.setattr(controller, '_tick', step)
    monkeypatch.setattr(controller.observers, 'tick', observers)

    async def exercise():
        """Let one real inventory cycle pass, then trigger either kind of required wakeup."""
        controller.start()
        try:
            for _ in range(300):
                if controller._idle_paused:
                    break
                await asyncio.sleep(.01)
            assert controller._idle_paused and ticks == ['paused']
            count = len(reads)
            await asyncio.sleep(2.2)
            assert ticks == ['paused'] and len(reads) == count
            assert original_read(OWNER, row['id'])['pending_ai'] == reservation
            if wake == 'resume':
                await asyncio.to_thread(store.update, OWNER, row['id'], lambda value: value.update(status='running'))
            else:
                monkeypatch.setattr(module.time, 'time', lambda: row['deadline'] - 59)
            for _ in range(300):
                if len(ticks) >= 2:
                    break
                await asyncio.sleep(.01)
            assert ticks[1] == ('running' if wake=='resume' else 'paused')
        finally:
            await controller.close()
        assert not controller._idle_paused

    asyncio.run(exercise())


def test_loop_archive_read_allows_unrelated_api_work(record, tmp_path, monkeypatch):
    """A slow archive read must yield to other requests before it completes."""
    store, row = record
    controller = LoopController(tmp_path, store=store, backend=FakeBackend())
    original = store.read

    async def scenario():
        """Hold one simulated disk read while the event loop services a heartbeat."""
        loop = asyncio.get_running_loop()
        entered = asyncio.Event()
        release = threading.Event()

        def slow_read(*args):
            """Never block the test indefinitely if storage regresses onto the API thread."""
            loop.call_soon_threadsafe(entered.set)
            assert release.wait(2), 'Archive read blocked unrelated API work'
            return original(*args)

        async def idle(_row):
            """Isolate the initial archive read from provider and worker operations."""

        monkeypatch.setattr(store, 'read', slow_read)
        monkeypatch.setattr(controller, '_tick', idle)
        monkeypatch.setattr(controller.observers, 'tick', idle)
        task = asyncio.create_task(controller.tick(OWNER, row['id']))
        try:
            await asyncio.wait_for(entered.wait(), 3)
            assert not task.done()
            await asyncio.sleep(0)
        finally:
            release.set()
            await task

    asyncio.run(scenario())


@pytest.mark.parametrize('operation', ['finish', 'observer'])
def test_loop_and_holdout_transactions_run_outside_api_thread(record, tmp_path, monkeypatch, operation):
    """Real temporary archive reads/writes stay off the loop, including keyword updates."""
    store, row = record
    row = reference(store, row)
    backend = Backend()
    backend.done = True
    controller = LoopController(tmp_path, store=store, backend=backend)
    api_thread = threading.get_ident()
    calls = []

    for name in ('read', 'write'):
        original = getattr(store, name)

        def checked(*args, _name=name, _original=original, **kwargs):
            """Check thread ownership while retaining actual locks and persistence."""
            assert threading.get_ident() != api_thread, f'{_name} blocked the API thread'
            calls.append(_name)
            return _original(*args, **kwargs)

        monkeypatch.setattr(store, name, checked)

    if operation == 'finish':
        asyncio.run(controller.finish(row, 'Training complete'))
    else:
        asyncio.run(ObserverJobs(store, backend, owned_thread).tick(row))
    assert 'read' in calls and 'write' in calls


def test_cancelled_storage_operation_keeps_owner_until_worker_finishes():
    """Shutdown waits for an already started keyword-bearing transaction to finish."""
    async def scenario():
        """Cancellation cannot abandon a worker while its caller releases ownership."""
        loop = asyncio.get_running_loop()
        entered = asyncio.Event()
        release = threading.Event()
        completed = threading.Event()

        def transaction(*, value):
            """Represent a held persistence lock without touching runtime files."""
            loop.call_soon_threadsafe(entered.set)
            assert release.wait(2)
            assert value == 7
            completed.set()

        task = asyncio.create_task(owned_thread(transaction, value=7))
        try:
            await asyncio.wait_for(entered.wait(), 3)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
        finally:
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert completed.is_set()

    asyncio.run(scenario())


@pytest.mark.parametrize('replacement', [False, True])
def test_legacy_inventory_reuses_small_summary_and_observes_file_changes(record, monkeypatch, replacement):
    """Unchanged historical runs are decoded once; writes/replacements invalidate summaries."""
    store, row = record
    path = store.path(OWNER, row['id'])
    row.update(status='stopped', cleanup_done=True)
    row['history'] = [{'round': 0, 'large_history': 'x' * 100_000}]
    path.write_text(json.dumps(row))
    original = store.read
    reads = []

    def counted(*args):
        """Measure historical hydration without reading production state."""
        reads.append(args)
        return original(*args)

    monkeypatch.setattr(store, 'read', counted)
    first = store.list_metadata()[0]
    assert first['_metadata_only'] and not first['observer_pending']
    assert 'history' not in first
    first['status'] = 'tampered'
    assert store.list_metadata()[0]['status'] == 'stopped'
    assert len(reads) == 1

    row.update(status='running', cleanup_done=False)
    if replacement:
        new_path = path.with_suffix('.replacement')
        new_path.write_text(json.dumps(row))
        new_path.replace(path)
    else:
        path.write_text(json.dumps(row))
    current = store.list_metadata()[0]
    assert current['status'] == 'running' and current['observer_pending']
    assert len(reads) == 2
    store.write(row)
    assert store.list_metadata()[0]['status'] == 'running'
    assert store.path(OWNER, row['id']).read_text().find('_loop_storage') >= 0


def test_legacy_inventory_cache_is_bounded_and_rejects_new_symlinks(record, tmp_path):
    """Historical summaries cannot grow without limit or bypass boundary validation."""
    store, row = record
    directory = store.owner_dir(OWNER)
    for number in range(129):
        legacy = {**row, 'id': f'{number:032x}', 'status': 'stopped', 'cleanup_done': True}
        (directory / (legacy['id'] + '.json')).write_text(json.dumps(legacy))
    store.list_metadata()
    assert len(store._metadata_cache) == 128
    path = next(reversed(store._metadata_cache))
    outside = tmp_path / 'outside.json'
    outside.write_text(path.read_text())
    path.unlink()
    path.symlink_to(outside)
    assert path.stem not in {item['id'] for item in store.list_metadata()}


def test_archive_reads_reuse_current_snapshot_and_detect_other_writer(record, monkeypatch):
    """Unchanged full reads avoid rehydration; a separate store's write is visible immediately."""
    from pb8_loop_storage import LoopArchive
    from pb8_loop_store import LoopStore
    store, row = record
    original = LoopArchive.load
    loads = []

    def counted(archive, root):
        """Count archive expansion independently of cache-owned copy operations."""
        loads.append(root)
        return original(archive, root)

    monkeypatch.setattr(LoopArchive, 'load', counted)
    for _ in range(5):
        current = store.read(OWNER, row['id'])
        current['initial_config']['live']['must_not_escape'] = True
    assert not loads
    assert 'must_not_escape' not in store.read(OWNER, row['id'])['initial_config']['live']
    other = LoopStore(store.root)
    other.update(OWNER, row['id'], lambda value: value.update(round=7))
    assert store.read(OWNER, row['id'])['round'] == 7
    assert len(loads) == 2
    store.read(OWNER, row['id'])
    assert len(loads) == 2


def test_archive_read_cache_expires_is_bounded_and_releases_on_shutdown(record, monkeypatch):
    """Bound snapshot bytes, entries and idle retention without retaining oversized runs."""
    import pb8_loop_store as module
    store, row = record
    assert store._read_cache
    clock = module.time.monotonic()
    monkeypatch.setattr(module.time, 'monotonic', lambda: clock + module.READ_CACHE_TTL + 1)
    from pb8_loop_storage import LoopArchive
    original = LoopArchive.load
    reads = []

    def counted(archive, root):
        """An expired entry must undergo fresh archive integrity verification."""
        reads.append(root)
        return original(archive, root)

    monkeypatch.setattr(LoopArchive, 'load', counted)
    store.read(OWNER, row['id'])
    assert len(reads) == 1
    monkeypatch.setattr(module, 'READ_CACHE_BYTES', 30_000)
    store.clear_read_cache()
    for number in range(20):
        path = store.root / str(number)
        store._remember_read(path, (number,), {'payload': 'x' * 2000})
    assert len(store._read_cache) <= 16
    assert store._read_cache_size <= 30_000
    store._remember_read(store.root / 'oversized', (), {'payload': 'x' * 20_000})
    assert store.root / 'oversized' not in store._read_cache
    store.clear_read_cache()
    assert not store._read_cache and store._read_cache_size == 0


def test_unchanged_observer_poll_does_not_rewrite_entire_loop(record, monkeypatch):
    """Repeated running-status polls leave history/archive bytes untouched."""
    store, row = record
    job = {'operation': 'b' * 32, 'status': 'running', 'started': True, 'error': None}
    row['observer_jobs'] = [job]
    observer = ObserverJobs(store, Backend(), owned_thread)

    def forbidden(*_args):
        """No state change warrants another archive transaction."""
        pytest.fail('Unchanged poll rewrote the entire archive')

    monkeypatch.setattr(store, 'update', forbidden)
    assert observer.update(row, job, status='running', started=True, error=None) is row


def test_loop_list_builds_projection_and_json_outside_api_thread(record, monkeypatch):
    """Returning a large loop list cannot pause unrelated API request handlers."""
    from types import SimpleNamespace
    from api import loop_optimizer_v8 as module
    store, row = record
    api_thread = threading.get_ident()
    calls = []
    original = store.read

    def listed(*args, **kwargs):
        """Capture the owning thread of filesystem inventory."""
        assert threading.get_ident() != api_thread
        return original(*args, **kwargs)

    def projected(row):
        """Projection and serialization belong to the same worker as inventory."""
        assert threading.get_ident() != api_thread
        calls.append(row['id'])
        return {'id': row['id']}

    monkeypatch.setattr(module, '_owner', lambda _session: OWNER)
    monkeypatch.setattr(store, 'read', listed)
    monkeypatch.setattr(module, '_controller', SimpleNamespace(store=store))
    monkeypatch.setattr(module, 'projection', projected)
    response = asyncio.run(module.list_loops(None))
    assert json.loads(response.body) == {'loops': [{'id': row['id']}]}
    assert calls == [row['id']]


def test_loop_polling_reuses_projection_but_tracks_writes_deletion_and_owner(record, monkeypatch):
    """Historical polling is cheap while external writes/deletion and owners remain authoritative."""
    from types import SimpleNamespace
    from api import loop_optimizer_v8 as module
    from pb8_loop_store import LoopStore
    store, row = record
    monkeypatch.setattr(module, '_controller', SimpleNamespace(store=store))
    original = store.read
    reads = []

    def counted(*args, **kwargs):
        """Count full state hydration without changing the real projection contract."""
        reads.append(args)
        return original(*args, **kwargs)

    module._clear_list_cache()
    monkeypatch.setattr(store, 'read', counted)
    first = module._list_response(OWNER).body
    assert module._list_response(OWNER).body == first
    assert len(reads) == 1
    assert json.loads(module._list_response('c' * 32).body) == {'loops': []}
    other = LoopStore(store.root)
    other.update(OWNER, row['id'], lambda value: value.update(round=17))
    current = json.loads(module._list_response(OWNER).body)
    assert current['loops'][0]['round'] == 17 and len(reads) == 2
    store.path(OWNER, row['id']).unlink()
    assert json.loads(module._list_response(OWNER).body) == {'loops': []}
    assert sum(len(entry[2]) for entry in module._list_cache.values()) <= module._LIST_CACHE_BYTES
    module._clear_list_cache()


def test_provider_ledger_runs_outside_api_thread(record, monkeypatch):
    """AI quota reservation, deadline reads and settlement cannot block the API loop."""
    from types import SimpleNamespace
    from pb8_loop_dialog import OpenCodeDialog
    from tests.test_pb8_loop_dialog_v2 import profile
    _store, row = record
    api_thread = threading.get_ident()
    called = []

    def reserve(*_args):
        """Represent the durable quota transaction."""
        assert threading.get_ident() != api_thread
        called.append('reserve')
        return 'call'

    def remaining():
        """Represent an authoritative persisted deadline read."""
        assert threading.get_ident() != api_thread
        called.append('remaining')
        return 60

    def settle(*_args):
        """Represent completed usage accounting."""
        assert threading.get_ident() != api_thread
        called.append('settle')

    async def send(*_args):
        """Complete the synthetic provider request without any network traffic."""
        return {'choices': [{'finish_reason': 'stop', 'message': {'content': '{"ok":true}'}}]}

    session = SimpleNamespace(required_pages=[], missing=lambda: [])
    ledger = SimpleNamespace(reserve=reserve, remaining=remaining, settle=settle)
    settings = {**row['settings'], 'provider': 'opencode-go', 'effort': '', 'service_tier': ''}
    dialog = OpenCodeDialog(settings, {'protocol': 'chat', 'context': 256000}, profile(), session, ledger, send)
    result = asyncio.run(dialog.run('Fixture', {'stage': 'select'}, 1000))
    assert json.loads(result) == {'ok': True}
    assert called == ['reserve', 'remaining', 'settle']


@pytest.mark.parametrize('action', ['pause', 'stop'])
def test_user_control_interrupts_only_the_owned_inflight_ai_request(record, tmp_path, monkeypatch, action):
    """A silent model must not hold a persisted pause/stop until its response arrives."""
    from types import SimpleNamespace
    store, row = record

    async def scenario():
        """Exercise the real metadata scan, task ownership and cancellation cleanup."""
        entered, cancelled = asyncio.Event(), asyncio.Event()
        calls = []

        async def decide(*_args):
            """Wait indefinitely as a provider can while reasoning without tool events."""
            calls.append('sent')
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        controller = LoopController(tmp_path, store=store, backend=FakeBackend(), ai=SimpleNamespace(decide=decide))
        controller._evidence_retention_at = float('inf')

        async def step(current):
            """Only an active row may dispatch a new synthetic model request."""
            if current['status'] in {'running', 'finishing'}:
                await controller.decision(current, 'evaluate', {})

        monkeypatch.setattr(controller, '_tick', step)
        controller.start()
        try:
            await asyncio.wait_for(entered.wait(), 3)
            await asyncio.to_thread(controller.action, OWNER, row['id'], action)
            await asyncio.wait_for(cancelled.wait(), 5)
        finally:
            await controller.close()
        assert calls == ['sent']
        assert not controller.restart_block_reason()
        assert store.read(OWNER, row['id'])['status'] == ('paused' if action == 'pause' else 'stopping')

    asyncio.run(scenario())
