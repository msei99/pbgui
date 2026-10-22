"""Offline archive durability, bounded active state, migration and resume regressions."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
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
