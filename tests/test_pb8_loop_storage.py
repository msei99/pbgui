"""Offline archive durability, bounded active state, migration and resume regressions."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import threading
import zlib
from types import SimpleNamespace

import pytest

from pb8_loop_controller import LoopController
from pb8_loop_storage import LoopArchive, LoopStorageError, ObjectSession, encoded
from pb8_loop_store import LoopStore, digest
from tests.test_pb8_loop_optimizer import FakeBackend, OWNER, record
from tests.test_pb8_loop_workflow import workflow


def archive_for(store, row):
    """Address only the temporary owner-scoped archive belonging to this fixture."""
    return LoopArchive(store.owner_dir(OWNER), row['id'])


@pytest.mark.parametrize('field', ['jobs', 'observer_jobs'])
@pytest.mark.parametrize('legacy', [False, True])
def test_native_job_navigation_never_loads_full_history_or_waits_on_raw_cache(record, monkeypatch, field, legacy):
    """Resolve archived/legacy targets while the controller owns its unrelated raw cache."""
    store, row = record
    operation = '7'*32
    target = {'operation': operation, 'name': 'Selected exact result', 'kind': 'observer_full_range',
              'backend': {'execution': 'validation', 'id': '77777777-7777-7777-7777-777777777777'}}
    row = store.update(OWNER, row['id'], lambda value: value.update(
        **{field: [{'operation': '6'*32, 'config': {'large': 'x'*32768}},
                   {**target, 'config': {'large': 'x'*32768}}]},
        history=[{'large_report': 'x'*32768} for _ in range(12)]))
    if legacy:
        store.path(OWNER, row['id']).write_text(json.dumps(row))

    def forbidden(*_args, **_kwargs):
        """Native navigation must not hydrate a run or use its shared raw snapshot cache."""
        pytest.fail('Native target navigation loaded the full run')

    unpack = ObjectSession.unpack

    def metadata_only(session, reference):
        """A target lookup must not deserialize any complete configuration object."""
        if reference[0] == 'ref' and session.object(reference)[0] == 'json':
            pytest.fail('Native target navigation loaded a configuration')
        return unpack(session, reference)

    monkeypatch.setattr(store, 'read', forbidden)
    monkeypatch.setattr(store, '_remember_read', forbidden)
    monkeypatch.setattr(LoopArchive, 'load', forbidden)
    monkeypatch.setattr(ObjectSession, 'unpack', metadata_only)
    with ThreadPoolExecutor(max_workers=1) as pool:
        with store._read_cache_lock:
            identity, actual = pool.submit(store.native_job, OWNER, row['id'], operation).result(timeout=2)
    assert identity == {'id': row['id'], 'owner': OWNER} and actual == target
    assert store.native_job(OWNER, row['id'], '9'*32)[1] is None
    with pytest.raises(FileNotFoundError):
        store.native_job('f'*32, row['id'], operation)
    with pytest.raises(ValueError):
        store.native_job(OWNER, row['id'], '../escape')


def test_native_job_checks_manifest_archive_identity_and_integrity(record):
    """A fast lookup must fail closed on substituted identities or corrupt referenced bytes."""
    store, row = record
    path = store.path(OWNER, row['id'])
    original = json.loads(path.read_text())
    path.write_text(json.dumps({**original, 'owner': 'f'*32}))
    with pytest.raises(ValueError, match='ownership mismatch'):
        store.native_job(OWNER, row['id'], '7'*32)
    archive = archive_for(store, row)
    other_root = archive.save({**row, 'owner': 'f'*32})
    path.write_text(json.dumps({**original, '_loop_storage': {'version': 1, 'root': other_root}}))
    with pytest.raises(LoopStorageError, match='ownership mismatch'):
        store.native_job(OWNER, row['id'], '7'*32)
    path.write_text(json.dumps(original))
    with archive.connection(write=True) as session:
        session.connection.execute('UPDATE objects SET payload = ? WHERE hash = ?',
                                   (zlib.compress(b'corrupt'), original['_loop_storage']['root'][1]))
    with pytest.raises(LoopStorageError, match='integrity verification'):
        store.native_job(OWNER, row['id'], '7'*32)


def test_display_read_skips_expensive_snapshot_cache_but_keeps_validation(record, monkeypatch):
    """Read-only display callers need no second raw-history copy or cache accounting."""
    store, row = record
    store.clear_read_cache()
    remembered = []
    original = store._remember_read

    def remember(*args):
        """Track the expensive production cache boundary with isolated data."""
        remembered.append(True)
        return original(*args)

    monkeypatch.setattr(store, '_remember_read', remember)
    fresh = store.read(OWNER, row['id'], cache_snapshot=False)
    assert fresh['id'] == row['id'] and remembered == []
    fresh['settings']['config_name'] = 'changed only in caller'
    assert store.read(OWNER, row['id'], cache_snapshot=False)['settings'] == row['settings']
    with pytest.raises(FileNotFoundError):
        store.read('f' * 32, row['id'], cache_snapshot=False)
    assert store.read(OWNER, row['id'])['id'] == row['id']
    assert remembered == [True]


def test_display_fields_skip_candidate_archive_and_preserve_complete_projection(record, monkeypatch):
    """Display reads return the same UI data without hydrating training candidates."""
    from api import loop_optimizer_v8 as api
    store, row = record
    store.update(OWNER, row['id'], lambda value: value.update(
        training_candidates=[{'unused': 'large evidence'*1000}], candidates=[{'unused': 'candidate'}]))
    expected = api.projection(store.read(OWNER, row['id']))
    load = LoopArchive.load

    def forbidden(*_args, **_kwargs):
        """The display path must select fields directly from the immutable archive."""
        pytest.fail('Display loaded the complete archive')

    monkeypatch.setattr(LoopArchive, 'load', forbidden)
    selected = store.read(OWNER, row['id'], fields=api._DISPLAY_FIELDS)
    assert 'training_candidates' not in selected and 'candidates' not in selected
    assert api.projection(selected) == expected
    monkeypatch.setattr(LoopArchive, 'load', load)
    assert store.read(OWNER, row['id'])['training_candidates']


def test_navigation_overview_never_loads_settings_history_or_projects_reports(record, monkeypatch):
    """The initial screen depends only on verified status/navigation metadata."""
    from api import loop_optimizer_v8 as api
    store, row = record
    row = store.update(OWNER, row['id'], lambda value: value['settings']['goals'].update(text='large user request '*10000))
    monkeypatch.setattr(api, '_controller', SimpleNamespace(store=store))
    archive = archive_for(store, row)
    header = json.loads(store.path(OWNER, row['id']).read_text())
    with archive.connection() as session:
        forbidden_references = [session.field(header['_loop_storage']['root'], field)
                                for field in ('settings', 'jobs', 'history', 'initial_config')]
    original = ObjectSession.unpack

    def bounded_unpack(session, reference):
        """Fail if a full settings/config/report collection reaches the overview path."""
        assert reference not in forbidden_references
        return original(session, reference)

    def forbidden(*_args, **_kwargs):
        """Neither raw-history reads nor report projection can block first paint."""
        pytest.fail('Overview performed report work')

    with monkeypatch.context() as patch:
        patch.setattr(ObjectSession, 'unpack', bounded_unpack)
        patch.setattr(api, 'projection', forbidden)
        patch.setattr(LoopArchive, 'load', forbidden)
        patch.setattr(store, '_remember_read', forbidden)
        response = api._overview_response(OWNER)
        assert len(response.body) < 5000
        item = json.loads(response.body)['loops'][0]
        assert item['summary_only'] and item['status'] == row['status']
        assert set(item['settings']) <= {'config_name', 'loop_name', 'execution'}
        assert item['jobs'] == [] and item['cycles'] == []
        assert item['view_revision'] and api._overview_response('f'*32).body == b'{"loops":[]}'
    detail = json.loads(api._detail_response(OWNER, row['id']).body)
    assert detail['view_revision'] == item['view_revision']
    store.update(OWNER, row['id'], lambda value: value.update(reason='new current state'))
    changed = json.loads(api._overview_response(OWNER).body)['loops'][0]
    assert changed['view_revision'] != item['view_revision']


def test_slow_owner_list_does_not_block_another_owners_cached_list(record, monkeypatch):
    """Expensive disk/projection work never holds the global list-cache lock."""
    from api import loop_optimizer_v8 as api
    store, row = record
    other = 'f'*32
    other_store_row = dict(row, owner=other, id='e'*32)
    store.write(other_store_row)
    monkeypatch.setattr(api, '_controller', SimpleNamespace(store=store))
    api._clear_list_cache()
    expected = api._list_response(other).body
    entered, release = threading.Event(), threading.Event()
    original = api.projection

    def slow_projection(value):
        """Hold one owner's cold build while another asks for an unchanged cached list."""
        if value['owner'] == OWNER:
            entered.set()
            assert release.wait(3)
        return original(value)

    monkeypatch.setattr(api, 'projection', slow_projection)
    # Cache keys deliberately include the projection identity.
    api._list_response(other)
    with ThreadPoolExecutor(max_workers=2) as pool:
        pending = pool.submit(api._list_response, OWNER)
        try:
            assert entered.wait(2)
            assert pool.submit(api._list_response, other).result(timeout=1).body == expected
        finally:
            release.set()
        pending.result(timeout=2)
    api._clear_list_cache()


def test_display_api_does_not_populate_raw_snapshot_cache(record, monkeypatch):
    """Polling and detail requests keep projections without copying full raw histories."""
    from api import loop_optimizer_v8 as api
    store, row = record
    monkeypatch.setattr(api, '_controller', SimpleNamespace(store=store))
    api._clear_list_cache()
    store.clear_read_cache()

    def forbidden(*_args):
        """Reproduce the expensive path as a hard failure for display requests."""
        raise AssertionError('Display request copied a full raw snapshot')

    monkeypatch.setattr(store, '_remember_read', forbidden)
    try:
        listing = api._list_response(OWNER)
        detail = api._detail_response(OWNER, row['id'])
        assert json.loads(listing.body)['loops'][0]['id'] == row['id']
        assert json.loads(detail.body)['id'] == row['id']
        assert listing.headers['cache-control'] == detail.headers['cache-control'] == 'no-store'
        assert api._list_response(OWNER).body == listing.body
    finally:
        api._clear_list_cache()


def test_unchanged_report_cache_is_owned_and_invalidates_changed_state(record, monkeypatch):
    """Repeat navigation reuses verified bytes; updates and foreign owners cannot reuse them."""
    from api import loop_optimizer_v8 as api
    store, row = record
    calls = []
    original = store.read

    def read(*args, **kwargs):
        """Count only report hydration, retaining the real transactional store for edits."""
        if kwargs.get('fields') == api._DISPLAY_FIELDS:
            calls.append(True)
        return original(*args, **kwargs)

    monkeypatch.setattr(api, '_controller', SimpleNamespace(store=store))
    monkeypatch.setattr(store, 'read', read)
    api._clear_list_cache()
    try:
        first = api._detail_response(OWNER, row['id'])
        clock = api.time.monotonic()
        monkeypatch.setattr(api.time, 'monotonic', lambda: clock + 31)
        assert api._detail_response(OWNER, row['id']).body == first.body and len(calls) == 1
        with pytest.raises(FileNotFoundError):
            api._detail_response('f'*32, row['id'])
        store.update(OWNER, row['id'], lambda value: value.update(round=42))
        updated = json.loads(api._detail_response(OWNER, row['id']).body)
        assert updated['round'] == 42 and updated['view_revision'] != json.loads(first.body)['view_revision']
        assert len(calls) == 3
    finally:
        api._clear_list_cache()


def test_detail_projection_and_serialization_leave_api_loop_responsive(record, monkeypatch):
    """A slow history projection must run in a worker while the API loop progresses."""
    from api import loop_optimizer_v8 as api
    store, row = record
    monkeypatch.setattr(api, '_controller', SimpleNamespace(store=store))
    monkeypatch.setattr(api, '_owner', lambda _session: OWNER)
    started, release = threading.Event(), threading.Event()
    projection_threads = []

    def slow_projection(value):
        """Hold the worker until an independent event-loop task releases it."""
        projection_threads.append(threading.get_ident())
        started.set()
        assert release.wait(2), 'Projection blocked the API event loop'
        return {'id': value['id']}

    monkeypatch.setattr(api, 'projection', slow_projection)

    async def scenario():
        """Never call a provider or native job from this scheduling test."""
        main_thread = threading.get_ident()
        task = asyncio.create_task(api.get_loop(row['id'], session=object()))
        try:
            assert await asyncio.to_thread(started.wait, 1)
            assert len(projection_threads) == 1 and projection_threads[0] != main_thread
        finally:
            release.set()
        assert json.loads((await task).body)['id'] == row['id']

    asyncio.run(scenario())


def test_thousand_round_archive_keeps_manifest_bounded_and_deduplicates_configs(record):
    """A logical state above the old 32-MiB limit must retain a constant-sized active JSON."""
    store, row = record
    config = {**row['initial_config'], 'archive_test_payload': 'x' * 32768}
    sizes = []
    for count in (1, 20, 200, 1000):
        row['round'] = count
        row['history'] = [{'round': i, 'config': config, 'metrics': {'gain': i + .123456789012345}}
                          for i in range(count)]
        row['ai_decisions'] = {str(i): {'round': i, 'tokens': i * 1000} for i in range(count)}
        store.write(row)
        raw = store.path(OWNER, row['id']).read_bytes()
        header = json.loads(raw)
        sizes.append(len(raw))
        assert 'history' not in header and 'ai_decisions' not in header and 'jobs' not in header
        assert len(header['_loop_storage']['root']) == 2
    assert max(sizes) < 2048 and max(sizes) - min(sizes) < 64
    assert len(json.dumps(row, indent=4).encode()) > 32 * 1024 * 1024
    archived = archive_for(store, row)
    key = hashlib.sha256(encoded(['json', config])).hexdigest()
    with archived.connection() as session:
        assert session.connection.execute('SELECT COUNT(*) FROM objects WHERE hash = ?', (key,)).fetchone()[0] == 1
        before = session.connection.execute('SELECT COUNT(*) FROM objects').fetchone()[0]
    store.write(row)
    with archived.connection() as session:
        assert session.connection.execute('SELECT COUNT(*) FROM objects').fetchone()[0] == before
    restored = LoopStore(store.root).read(OWNER, row['id'])
    assert len(restored['history']) == 1000 and restored['history'][-1] == row['history'][-1]
    assert archived.path.stat().st_mode & 0o777 == 0o600
    assert archived.directory.stat().st_mode & 0o777 == 0o700


def test_archive_page_skips_older_rounds_and_does_not_expand_whole_run(record, monkeypatch):
    """A late page reads bounded tree branches, not all previous historical configs."""
    store, row = record
    row['history'] = [{'round': i, 'metrics': {'gain': i + .25}} for i in range(5000)]
    store.write(row)
    monkeypatch.setattr(LoopArchive, 'load', lambda *_args: pytest.fail('Full hydration is forbidden for archive pages'))
    reads = []
    original = ObjectSession.object

    def counted(session, reference):
        """Measure archive traversal without affecting native result files."""
        reads.append(reference)
        return original(session, reference)

    monkeypatch.setattr(ObjectSession, 'object', counted)
    page = store.archive_page(OWNER, row['id'], 'history', 4996, 3)
    assert [item['round'] for item in page['items']] == [4996, 4997, 4998]
    assert page['total'] == 5000 and page['next_offset'] == 4999
    assert len(reads) < 50
    assert store.archive_page(OWNER, row['id'], 'history', 5000, 3)['items'] == []


def test_legacy_migration_preserves_exact_original_bytes_and_read_has_no_side_effect(record):
    """The first normal write migrates atomically; a legacy read does not rewrite runtime data."""
    store, row = record
    path = store.path(OWNER, row['id'])
    original = json.dumps(row, indent=4).encode() + b'\n'
    path.write_bytes(original)
    assert store.read(OWNER, row['id'])['id'] == row['id']
    assert path.read_bytes() == original
    store.update(OWNER, row['id'], lambda current: current.update(round=1))
    assert '_loop_storage' in json.loads(path.read_bytes())
    with archive_for(store, row).connection() as session:
        saved = session.connection.execute('SELECT payload FROM legacy WHERE hash = ?',
                                          (hashlib.sha256(original).hexdigest(),)).fetchone()
    assert zlib.decompress(saved[0]) == original
    assert store.read(OWNER, row['id'])['round'] == 1


def test_failed_manifest_publication_keeps_previous_root_and_all_old_data(record, monkeypatch):
    """A crash after archive commit cannot publish an incomplete transition or lose the prior state."""
    import pb8_loop_store
    store, row = record
    path = store.path(OWNER, row['id'])
    before = path.read_bytes()
    original_write = pb8_loop_store.atomic_write_private_text

    def fail_manifest(destination, content):
        """Simulate a local disk error only at the authoritative active-state swap."""
        if destination == path:
            raise OSError('test disk error')
        original_write(destination, content)

    monkeypatch.setattr(pb8_loop_store, 'atomic_write_private_text', fail_manifest)
    with pytest.raises(LoopStorageError):
        store.update(OWNER, row['id'], lambda current: current.update(round=123))
    assert path.read_bytes() == before
    assert store.read(OWNER, row['id'])['round'] == row['round']
    monkeypatch.setattr(pb8_loop_store, 'atomic_write_private_text', original_write)
    store.update(OWNER, row['id'], lambda current: current.update(round=1))
    assert store.read(OWNER, row['id'])['round'] == 1


def test_configuration_roundtrip_does_not_alias_deduplicated_mutable_objects(record):
    """Two references to the same stored config remain independently editable after restart."""
    store, row = record
    row['best'] = {'config': row['initial_config']}
    row['baseline'] = {'config': row['initial_config']}
    store.write(row)
    restored = store.read(OWNER, row['id'])
    restored['best']['config']['bot']['long']['risk']['n_positions'] = 999
    assert restored['baseline']['config']['bot']['long']['risk']['n_positions'] != 999
    assert store.read(OWNER, row['id'])['initial_config'] == row['initial_config']


def test_cached_read_decoding_does_not_block_another_reader(record, monkeypatch):
    """An expensive caller copy must not monopolize the shared snapshot-cache lock."""
    store, row = record
    entered, release = threading.Event(), threading.Event()
    slow_thread = []
    original = json.loads

    def decode(value, *args, **kwargs):
        """Hold one caller's decode while another reads the same verified snapshot."""
        if slow_thread and threading.get_ident() == slow_thread[0]:
            entered.set()
            assert release.wait(5)
        return original(value, *args, **kwargs)

    def slow_read():
        """Identify only the worker deliberately held at its deserialization boundary."""
        slow_thread.append(threading.get_ident())
        return store.read(OWNER, row['id'])

    monkeypatch.setattr(json, 'loads', decode)
    with ThreadPoolExecutor(max_workers=2) as pool:
        slow = pool.submit(slow_read)
        try:
            assert entered.wait(5)
            fast = pool.submit(store.read, OWNER, row['id']).result(timeout=2)
            fast['settings']['config_name'] = 'private caller edit'
        finally:
            release.set()
        assert slow.result(timeout=5)['settings'] == row['settings']
    assert store.read(OWNER, row['id'])['settings'] == row['settings']


def test_snapshot_cache_is_bounded_by_retained_bytes(record, monkeypatch):
    """Oversize snapshots stay readable without displacing the cache's memory bound."""
    import pb8_loop_store
    store, row = record
    store.clear_read_cache()
    monkeypatch.setattr(pb8_loop_store, 'READ_CACHE_BYTES', 256)
    assert store.read(OWNER, row['id'])['initial_config'] == row['initial_config']
    assert store._read_cache_size <= 256 and not store._read_cache


def test_concurrent_state_updates_do_not_lose_committed_counters(record):
    """Archive publication remains inside the existing reentrant process/file transaction lock."""
    store, row = record
    store.update(OWNER, row['id'], lambda current: current.update(storage_test_counter=0))

    def increment(_index):
        """Each worker uses its own store facade against the same temporary files."""
        local = LoopStore(store.root)
        local.update(OWNER, row['id'], lambda current: current.update(storage_test_counter=current['storage_test_counter'] + 1))

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(increment, range(24)))
    assert store.read(OWNER, row['id'])['storage_test_counter'] == 24


def test_header_scan_never_loads_archived_history(record, monkeypatch):
    """Controller inventory can skip collected historical runs without expanding them."""
    store, row = record
    monkeypatch.setattr(LoopArchive, 'load', lambda *_args: pytest.fail('Metadata scan hydrated the run'))
    rows = store.list_metadata()
    assert len(rows) == 1 and rows[0]['id'] == row['id'] and rows[0]['_metadata_only']
    assert 'history' not in rows[0]


def test_corrupt_archive_is_rejected_without_overwriting_manifest(record):
    """An altered SQLite object cannot bypass content integrity or masquerade as empty history."""
    store, row = record
    path = store.path(OWNER, row['id'])
    before = path.read_bytes()
    root = json.loads(before)['_loop_storage']['root'][1]
    archive = archive_for(store, row)
    with archive.connection(write=True) as session:
        session.connection.execute('UPDATE objects SET payload = ? WHERE hash = ?', (zlib.compress(b'[]'), root))
    with pytest.raises(LoopStorageError, match='integrity'):
        store.read(OWNER, row['id'])
    assert path.read_bytes() == before


def test_archive_symlink_and_foreign_owner_are_rejected(record, tmp_path):
    """Archives cannot redirect storage or expose another owner's root."""
    store, row = record
    archive = archive_for(store, row)
    archive.path.unlink()
    outside = tmp_path / 'outside'
    outside.write_text('unchanged')
    archive.path.symlink_to(outside)
    with pytest.raises((LoopStorageError, ValueError, RuntimeError)):
        store.write(row)
    assert outside.read_text() == 'unchanged'
    with pytest.raises(FileNotFoundError):
        store.read('b' * 32, row['id'])


def test_storage_failure_preserves_cached_ai_decision_and_resumes_without_another_call(record, tmp_path, monkeypatch):
    """An archive failure pauses locally; it is never sent to AI as a repair prompt."""
    store, row = record
    evidence = {'remaining_runs': 10}
    decision = {'winner_operation': None, 'finish': False}
    cached = {'stage': 'evaluate', 'round': row['round'], 'evidence_digest': digest(evidence),
              'repair_error': None, 'decision': decision}
    row = store.update(OWNER, row['id'], lambda current: current.update(status='running', phase='evaluate',
        repair_error=None, last_ai_decision=cached, history=[], errors=0))

    class NoNewAI:
        """Any paid/subscription request here would repeat a completed decision."""
        async def decide(self, *_args):
            """Reject any attempt to ask AI to repair local storage."""
            pytest.fail('Storage errors must not cause a new AI decision')

    controller = LoopController(tmp_path, backend=FakeBackend(), ai=NoNewAI(), store=store)
    original_save = LoopArchive.save
    monkeypatch.setattr(LoopArchive, 'save', lambda *_args, **_kwargs: (_ for _ in ()).throw(LoopStorageError('test archive full')))

    async def commit_result(current):
        """Represent the post-AI evaluation commit that failed in the real incident."""
        controller.commit(current, lambda saved: saved['history'].append({'round': saved['round'], 'reason': 'completed'}))

    async def no_observers(_record):
        """Never launch a native backtest from this persistence-only regression."""

    monkeypatch.setattr(controller, '_tick', commit_result)
    monkeypatch.setattr(controller.observers, 'tick', no_observers)
    asyncio.run(controller.tick(OWNER, row['id']))
    paused = store.read(OWNER, row['id'])
    assert paused['status'] == 'paused' and paused['ai_pause']['reason'] == 'storage'
    assert paused['last_ai_decision'] == cached and paused['repair_error'] is None and paused['errors'] == 0
    monkeypatch.setattr(LoopArchive, 'save', original_save)
    resumed = controller.action(OWNER, row['id'], 'resume')
    assert asyncio.run(controller.decision(resumed, 'evaluate', evidence)) == decision
    asyncio.run(controller.tick(OWNER, row['id']))
    assert len(store.read(OWNER, row['id'])['history']) == 1


def test_owned_archive_api_pages_history_and_preserves_account_boundary(record, workflow):
    """The archive endpoint bounds page sizes and never exposes another account's run."""
    _, row = record
    client, controller, _source, session, _preflights = workflow
    row['history'] = [{'round': i} for i in range(12)]
    controller.store.write(row)
    url = f'/loops/{row["id"]}/archive/history'
    response = client.get(url + '?offset=8&limit=3')
    assert response.status_code == 200 and response.headers['cache-control'] == 'no-store'
    assert response.json() == {'items': [{'round': i} for i in (8, 9, 10)],
                               'total': 12, 'offset': 8, 'next_offset': 11}
    assert client.get(url + '?limit=201').status_code == 422
    assert client.get(url + '?offset=-1').status_code == 422
    assert client.get(f'/loops/{row["id"]}/archive/initial_config').status_code == 422
    session.user_id = 'b' * 32
    assert client.get(url).status_code == 404


def test_support_export_keeps_archived_failure_details_and_redaction(record, tmp_path, monkeypatch):
    """Selected-run diagnostics use bounded archive pages and never export raw configurations."""
    import support_debug
    store, row = record
    row.update(status='failed', phase='evaluate', reason='Local storage failure',
               history=[{'round': i, 'reason': f'round {i}'} for i in range(200)],
               jobs=[{'operation': 'f' * 32, 'round': row['round'], 'kind': 'optimizer', 'status': 'failed',
                      'name': 'isolated', 'error': 'local persistence failed', 'backend': {},
                      'config': {'api_key': 'NEVER_EXPORT_THIS'}}])
    store.write(row)
    monkeypatch.setattr(support_debug.subprocess, 'run', lambda *_a, **_kw: SimpleNamespace(returncode=0, stdout=''))
    result = support_debug.collect(tmp_path, OWNER, [], loop_id=row['id'])
    selected = result['records']['selected_loop']
    assert selected['reason'] == 'Local storage failure'
    assert selected['jobs'][0]['error'] == 'local persistence failed'
    assert [item['round'] for item in selected['history']] == [198, 199]
    assert 'NEVER_EXPORT_THIS' not in json.dumps(result)


def test_hot_display_projection_reuses_verified_bytes_with_independent_values(record, monkeypatch):
    """Controller snapshots serve current reports without rehydrating the archive."""
    store, row = record
    fields = {'status', 'settings', 'history'}
    expected = store.read(OWNER, row['id'], fields=fields, settings_fields={'config_name'})
    store.read(OWNER, row['id'])
    original = LoopArchive.fields
    reads = []

    def observe(*args, **kwargs):
        """Count actual selective archive hydration rather than elapsed time."""
        reads.append(True)
        return original(*args, **kwargs)

    monkeypatch.setattr(LoopArchive, 'fields', observe)
    actual = store.read(OWNER, row['id'], fields=fields, settings_fields={'config_name'}, prefer_cached_fields=True)
    assert actual == expected and reads == []
    actual['settings']['config_name'] = 'caller-local'
    actual['history'].append({'caller': True})
    assert store.read(OWNER, row['id'], fields=fields, settings_fields={'config_name'}, prefer_cached_fields=True) == expected
    assert store.read(OWNER, row['id'], fields=fields, settings_fields={'config_name'}, prefer_cached_fields=True, cache_snapshot=False) == expected
    assert reads == [True]
    updated = store.update(OWNER, row['id'], lambda value: value.update(status='paused'))
    assert updated['status'] == 'paused'
    assert store.read(OWNER, row['id'], fields=fields, prefer_cached_fields=True)['status'] == 'paused'


def test_hot_display_read_never_waits_for_controller_cache_lock(record):
    """A display read falls back to verified archive fields when another thread owns cache."""
    store, row = record
    expected = store.read(OWNER, row['id'], fields={'status', 'settings'})
    with ThreadPoolExecutor(max_workers=1) as pool:
        with store._read_cache_lock:
            actual = pool.submit(store.read, OWNER, row['id'], fields={'status', 'settings'}, prefer_cached_fields=True).result(timeout=3)
    assert actual == expected


@pytest.mark.parametrize('corruption', ['owner', 'archive', 'symlink'])
def test_hot_display_cache_cannot_hide_identity_or_archive_tampering(record, corruption):
    """Hot snapshots are invalidated before returning data from changed storage."""
    store, row = record
    store.read(OWNER, row['id'])
    path = store.path(OWNER, row['id'])
    header = json.loads(path.read_text())
    if corruption == 'owner':
        path.write_text(json.dumps({**header, 'owner': 'f'*32}))
    elif corruption == 'symlink':
        replacement = path.with_suffix('.substitute')
        path.rename(replacement)
        path.symlink_to(replacement)
    else:
        with archive_for(store, row).connection(write=True) as session:
            session.connection.execute('UPDATE objects SET payload = ? WHERE hash = ?',
                                      (zlib.compress(b'corrupt'), header['_loop_storage']['root'][1]))
    with pytest.raises((ValueError, FileNotFoundError, LoopStorageError)):
        store.read(OWNER, row['id'], fields={'status', 'settings'}, prefer_cached_fields=True)
