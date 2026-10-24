"""Offline rental lifecycle and untrusted result transfer regression tests."""
import copy
from vast_direction_fixtures import direction_config, complete_direction
from vast_integrity_fixtures import freeze_test_proof
import io
import json
import shutil
import tarfile
from pathlib import Path

import msgpack
import pytest

from secure_files import ensure_private_directory
from vast_jobs import JobStore, IMAGE, REVISION, native_job_config, write_json, digest, _write_input_archive
from vast_job_runner import guard_step, run_loop, validate_intent
from vast_provider import VastError
from vast_transfer import extract_results, import_results, fetch_host_key_result
from setup.vast_gpu_benchmark.cloud_worker import safe_path


def final_result_fixture(store, identifier):
    """Write tiny native results and manifest exclusively below a test store."""
    final = ensure_private_directory(store.directory(identifier) / 'final-results')
    result = ensure_private_directory(final / 'optimize_results/run')
    (result / 'all_results.bin').write_bytes(msgpack.packb({'metrics': {}}))
    write_json(final / 'finished.json', {'exit_code': 0, 'wall_seconds': 12, 'finished_at': 1234})
    write_json(ensure_private_directory(store.directory(identifier) / 'input') / 'manifest.json', {})
    store.update(identifier, final_collected=True)
    return final


@pytest.mark.parametrize('failure', ['observe', 'status', 'persistence', 'import'])
def test_finalization_retry_after_publication_and_lock_release(job, monkeypatch, failure):
    """Technical failures preserve raw/published results, release Finalize and retry reentrantly."""
    import threading
    from concurrent.futures import ThreadPoolExecutor
    from file_lock import advisory_file_lock
    import vast_job_runner as runner
    import vast_convergence
    store, identifier, _ = job
    final = final_result_fixture(store, identifier)
    update = store.update
    importer = runner.import_results
    observe = vast_convergence.observe
    raised = False

    def broken_update(identifier, **changes):
        """Fail the final status write, and optionally its retry-state write."""
        nonlocal raised
        if changes.get('status') == 'completed' and not raised:
            raised = True
            raise RuntimeError('original failure')
        if failure == 'persistence' and changes.get('error') == 'Raw results saved locally; result import needs retry':
            raise ValueError('secondary failure')
        return update(identifier, **changes)

    def broken(*args, **kwargs):
        """Inject a technical error without touching production data."""
        raise RuntimeError('original failure')

    monkeypatch.setattr(runner, '_log', lambda *args, **kwargs: None)
    if failure in {'status', 'persistence'}:
        monkeypatch.setattr(store, 'update', broken_update)
    elif failure == 'observe':
        monkeypatch.setattr(vast_convergence, 'observe', broken)
    else:
        monkeypatch.setattr(runner, 'import_results', broken)
    with pytest.raises(RuntimeError, match='original failure'):
        runner.finalize_collected_results(store, identifier)
    state = store.read(identifier)
    if failure != 'persistence':
        assert state['status'] == 'failed'
        assert state['error'] == 'Raw results saved locally; result import needs retry'
    if failure != 'import':
        assert Path(state['result_path']).is_dir() and state['result_partial'] is False
    assert (final / 'optimize_results/run/all_results.bin').is_file()
    assert (store.directory(identifier) / '.finalize-lock.lock').is_file()
    monkeypatch.setattr(store, 'update', update)
    monkeypatch.setattr(runner, 'import_results', importer)
    monkeypatch.setattr(vast_convergence, 'observe', observe)

    def retry():
        """A different thread obtains the released lock, then enters it reentrantly."""
        with advisory_file_lock(store.directory(identifier) / '.finalize-lock'):
            return runner.finalize_collected_results(store, identifier)

    with ThreadPoolExecutor(max_workers=1) as executor:
        assert executor.submit(retry).result(timeout=5)['status'] == 'completed'


@pytest.mark.parametrize('caller', ['api', 'runner', 'stop'])
def test_finalization_waiter_reads_completed_state_before_expired_budget(job, monkeypatch, caller):
    """API/API, API/Runner and API/Stop share one transaction through final status."""
    import threading
    from contextlib import contextmanager
    from concurrent.futures import ThreadPoolExecutor
    import vast_job_runner as runner
    store, identifier, intent = job
    final_result_fixture(store, identifier)
    entered, release, waiting = threading.Event(), threading.Event(), threading.Event()
    original_import, original_lock = runner.import_results, runner.advisory_file_lock
    count = []

    def held_import(*args, **kwargs):
        """Hold publication while a second owner is waiting for Finalize."""
        count.append(1)
        entered.set()
        assert release.wait(5)
        return original_import(*args, **kwargs)

    @contextmanager
    def tracked_lock(target):
        """Observe the waiter before it blocks, without replacing the real lock."""
        if Path(target).name == '.finalize-lock' and threading.current_thread().name.endswith('_1'):
            waiting.set()
        with original_lock(target):
            yield

    def expired():
        """A completed outcome must bypass a now-expired cooperative deadline."""
        pytest.fail('Completed finalization re-entered its expired import budget')

    def waiter():
        """Exercise the actual public callers while the first import is held."""
        if caller == 'api':
            return runner.finalize_collected_results(store, identifier, check=expired)
        if caller == 'runner':
            runner.run_loop(store, identifier)
        else:
            store.update(identifier, stop_rpc_at=1, final_collection_at=1)
            runner.stop_and_collect(object(), store, identifier, intent)
        return store.read(identifier)

    monkeypatch.setattr(runner, 'import_results', held_import)
    monkeypatch.setattr(runner, 'advisory_file_lock', tracked_lock)
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(runner.finalize_collected_results, store, identifier)
        try:
            assert entered.wait(5)
            second = executor.submit(waiter)
            assert waiting.wait(5)
        finally:
            release.set()
        assert first.result(timeout=5)['status'] == 'completed'
        assert second.result(timeout=5)['status'] == 'completed'
    assert count == [1]
    assert store.read(identifier)['error'] is None


def test_startup_repair_waits_for_concurrent_finalization(job, monkeypatch):
    """Startup's stale empty-result candidate waits for API finalization and rechecks success."""
    import threading
    from contextlib import contextmanager
    from concurrent.futures import ThreadPoolExecutor
    import vast_job_runner as runner
    store, identifier, _ = job
    final = final_result_fixture(store, identifier)
    binary = final / 'optimize_results/run/all_results.bin'
    binary.write_bytes(b'')
    write_json(final / 'finished.json', {'cancelled': True, 'exit_code': 130})
    store.update(identifier, status='failed', stop_reason='requested', error='Raw results saved locally; result import needs retry')
    acquired, release, waiting = threading.Event(), threading.Event(), threading.Event()
    original_lock = runner.advisory_file_lock

    @contextmanager
    def tracked_lock(target):
        """Observe startup waiting on the same production lock."""
        if threading.current_thread().name.endswith('_1'):
            waiting.set()
        with original_lock(target):
            yield

    def api_finalization():
        """Repair evidence while owning Finalize, before the startup writer can inspect it."""
        with original_lock(store.directory(identifier) / '.finalize-lock'):
            acquired.set()
            assert release.wait(5)
            binary.write_bytes(msgpack.packb({'metrics': {}}))
            write_json(final / 'finished.json', {'exit_code': 0})
            return runner.finalize_collected_results(store, identifier)

    monkeypatch.setattr(runner, 'advisory_file_lock', tracked_lock)
    with ThreadPoolExecutor(max_workers=2) as executor:
        api = executor.submit(api_finalization)
        try:
            assert acquired.wait(5)
            startup = executor.submit(runner.recover_cancelled_empty_results, store)
            assert waiting.wait(5)
        finally:
            release.set()
        assert api.result(timeout=5)['status'] == 'completed'
        assert startup.result(timeout=5) == 0
    assert store.read(identifier)['status'] == 'completed'


def _process_finalizer(root, identifier, connection):
    """Finalize in a separate process; exchange only bounded test synchronization messages."""
    import vast_job_runner as runner
    runner._log = lambda *args, **kwargs: None
    original = runner.import_results

    def held_import(*args, **kwargs):
        """The parent releases publication after observing the competing waiter."""
        connection.send('import')
        assert connection.poll(8)
        assert connection.recv() == 'release'
        return original(*args, **kwargs)

    runner.import_results = held_import
    try:
        connection.send(runner.finalize_collected_results(JobStore(root), identifier)['status'])
    finally:
        connection.close()


def test_finalization_process_serialization(job, monkeypatch):
    """The actual .finalize-lock.lock serializes processes and leaves no child or pipe behind."""
    import multiprocessing
    import threading
    from contextlib import contextmanager
    from concurrent.futures import ThreadPoolExecutor
    import vast_job_runner as runner
    store, identifier, _ = job
    final_result_fixture(store, identifier)
    ctx = multiprocessing.get_context('spawn')
    parent, child = ctx.Pipe()
    process = ctx.Process(target=_process_finalizer, args=(store.root, identifier, child))
    waiting = threading.Event()
    original_lock = runner.advisory_file_lock

    @contextmanager
    def tracked_lock(target):
        """Observe the parent waiter while retaining the production file lock."""
        if Path(target).name == '.finalize-lock':
            waiting.set()
        with original_lock(target):
            yield

    process.start()
    child.close()
    try:
        assert parent.poll(8) and parent.recv() == 'import'
        monkeypatch.setattr(runner, 'advisory_file_lock', tracked_lock)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(runner.finalize_collected_results, store, identifier)
            try:
                assert waiting.wait(5)
                assert not future.done()
            finally:
                parent.send('release')
            assert future.result(timeout=8)['status'] == 'completed'
        assert parent.poll(8) and parent.recv() == 'completed'
        process.join(8)
        assert process.exitcode == 0
    finally:
        if process.is_alive():
            process.terminate()
        process.join(8)
        parent.close()


@pytest.mark.parametrize('field,value', [('result_partial', True), ('exact_completed', 999),
    ('completion_reason', 'requested'), ('finished_at', -1), ('status', 'failed'),
    ('error', 'Raw results saved locally; result import needs retry')])
def test_incomplete_published_outcomes_are_refinalized(job, monkeypatch, field, value):
    """A path or terminal status cannot bypass missing or contradictory completion evidence."""
    import vast_job_runner as runner
    store, identifier, _ = job
    final_result_fixture(store, identifier)
    runner.finalize_collected_results(store, identifier)
    store.update(identifier, **{field: value})
    imported, original = [], runner.import_results

    def counted(*args, **kwargs):
        """Count real idempotent import attempts."""
        imported.append(True)
        return original(*args, **kwargs)

    monkeypatch.setattr(runner, 'import_results', counted)
    assert runner.finalize_collected_results(store, identifier)['status'] == 'completed'
    assert imported == [True]


@pytest.mark.parametrize('checked', [999, None, True, 1.0])
def test_final_waiting_convergence_rejects_invalid_count(job, monkeypatch, checked):
    """Waiting cannot hide a contradictory or non-integer final evaluation counter."""
    import vast_convergence
    import vast_job_runner as runner
    store, identifier, _ = job
    final_result_fixture(store, identifier)
    store.update(identifier, convergence_config=dict(vast_convergence.DEFAULTS, convergence_enabled=True))
    first = runner.finalize_collected_results(store, identifier)
    assert first['evaluations'] == 1
    history = dict(first['convergence'])
    history['final'] = dict(phase='waiting', reason='Waiting for feasible exact results',
                            checked_exact=checked, threshold_reached=False)
    store.update(identifier, convergence=history)
    calls, observe = [], vast_convergence.observe

    def counted(*args, **kwargs):
        """Count actual final assessments rather than just import calls."""
        calls.append(kwargs.get('final'))
        return observe(*args, **kwargs)

    monkeypatch.setattr(vast_convergence, 'observe', counted)
    repeated = runner.finalize_collected_results(store, identifier)
    assert calls == [True]
    assert type(repeated['convergence']['final']['checked_exact']) is int
    assert repeated['convergence']['final']['checked_exact'] == repeated['evaluations']


@pytest.mark.parametrize('unavailable', [False, True])
def test_valid_final_waiting_convergence_skips_expired_import(job, monkeypatch, unavailable):
    """A matching count or genuinely unavailable assessment remains a complete outcome."""
    import vast_convergence
    import vast_job_runner as runner
    store, identifier, _ = job
    final = final_result_fixture(store, identifier)
    if unavailable:
        front = ensure_private_directory(final / 'optimize_results/run/pareto')
        (front / 'broken.json').write_text('{')
    store.update(identifier, convergence_config=dict(vast_convergence.DEFAULTS,
                 convergence_enabled=True, convergence_min_exact=1))
    first = runner.finalize_collected_results(store, identifier)
    assessment = first['convergence']['final']
    assert assessment['phase'] == 'waiting'
    assert ('checked_exact' not in assessment) is unavailable

    def forbidden(*args, **kwargs):
        """Completed evidence is checked before another import or depleted deadline."""
        pytest.fail('A valid completed waiting assessment was imported again')

    monkeypatch.setattr(runner, 'import_results', forbidden)
    repeated = runner.finalize_collected_results(store, identifier, check=forbidden)
    assert repeated['convergence']['final'] == assessment


@pytest.mark.parametrize('after_collection', [False, True])
def test_stop_collection_error_preserves_concurrent_recover(job, monkeypatch, after_collection):
    """A failure between collection and finalization cannot overwrite a completed API retry."""
    import vast_job_runner as runner
    store, identifier, intent = job
    monkeypatch.setattr(runner.time, 'time', lambda: 1100)
    store.update(identifier, stop_rpc_at=1000, final_collection_at=1100)

    class Connection:
        """Publish final evidence while Stop holds an older state snapshot."""
        def collect(self, final, **kwargs):
            """Model collection completion followed by a technical controller failure."""
            final_result_fixture(store, identifier)
            if after_collection:
                runner.finalize_collected_results(store, identifier)
            raise RuntimeError('isolated collection error')

    assert runner.stop_and_collect(Connection(), store, identifier, intent)
    row = store.read(identifier)
    # A verified Stop snapshot proves execution independently of publication.
    # Concurrent successful recovery must also keep its technical success state.
    assert row['status'] == 'completed' and row['error'] is None
    assert bool(row.get('collection_incomplete')) is (not after_collection)
    assert bool(row.get('collection_error')) is (not after_collection)
    assert store.read(identifier, 'control.json')['cleanup']


@pytest.mark.parametrize('log_failure', ['stat', 'open', 'read'])
@pytest.mark.parametrize('case', ['no_results', 'results', 'publication_error'])
def test_stop_execution_failure_survives_unreadable_diagnostic_log(job, monkeypatch, log_failure, case):
    """Optional log I/O cannot hide exit code 1 or block local publication recovery."""
    import vast_job_runner as runner
    store, identifier, intent = job
    final = final_result_fixture(store, identifier)
    write_json(final / 'finished.json', dict(exit_code=1, cancelled=False, finished_at=1099, wall_seconds=12))
    if case == 'no_results':
        (final / 'optimize_results/run/all_results.bin').unlink()
    diagnostic = final / 'optimizer.log'
    diagnostic.write_text('isolated optimizer diagnostic')
    store.update(identifier, kind='optimizer', status='collecting', stop_reason='requested',
                 stop_rpc_at=1000, final_collection_at=1100)
    monkeypatch.setattr(runner.time, 'time', lambda: 1100)
    warnings = []
    monkeypatch.setattr(runner, '_log', lambda service, message, **kwargs: warnings.append(message))
    original_open, original_is_file = Path.open, Path.is_file

    class UnreadableLog:
        """Fail a diagnostic read without opening a production file or retaining a handle."""
        def __enter__(self):
            """Provide the read context expected by the diagnostic enrichment."""
            return self

        def __exit__(self, *args):
            """No real resource was acquired."""

        def read(self, *args):
            """Represent a bounded diagnostic read encountering a storage error."""
            raise OSError('isolated diagnostic read failure')

    def checked_open(path, *args, **kwargs):
        """Restrict the injected error to the temporary optional optimizer log."""
        if path == diagnostic and log_failure != 'stat':
            if log_failure == 'open':
                raise PermissionError('isolated diagnostic open failure')
            return UnreadableLog()
        return original_open(path, *args, **kwargs)

    def checked_is_file(path):
        """Exercise inaccessible metadata separately from open and read errors."""
        if path == diagnostic and log_failure == 'stat':
            raise PermissionError('isolated diagnostic metadata failure')
        return original_is_file(path)

    monkeypatch.setattr(Path, 'open', checked_open)
    monkeypatch.setattr(Path, 'is_file', checked_is_file)
    importer = runner.import_results

    def publication_error(*args, **kwargs):
        """Local publication may fail independently of the optional diagnostic."""
        raise VastError('isolated publication failure')

    if case == 'publication_error':
        monkeypatch.setattr(runner, 'import_results', publication_error)
    assert runner.stop_and_collect(object(), store, identifier, intent)
    current = store.read(identifier)
    assert current['status'] == 'failed' and current['exit_code'] == 1
    assert current['error'].startswith('PB8 optimizer exited with code 1')
    assert bool(current['collection_incomplete']) is (case == 'publication_error')
    assert bool(current['collection_error']) is (case == 'publication_error')
    assert bool(current.get('stop_finalization_pending')) is (case == 'publication_error')
    assert any('diagnostic' in message.lower() for message in warnings)
    if case == 'publication_error':
        with pytest.raises(VastError, match='isolated publication failure'):
            runner.finalize_collected_results(store, identifier)
        assert store.read(identifier)['error'].startswith('PB8 optimizer exited with code 1')
        monkeypatch.setattr(runner, 'import_results', importer)
        for _ in range(2):
            recovered = runner.finalize_collected_results(store, identifier)
            assert recovered['status'] == 'failed' and recovered['exit_code'] == 1
            assert recovered['error'].startswith('PB8 optimizer exited with code 1')
            assert recovered['collection_incomplete'] is False and recovered['collection_error'] is None
            assert recovered['stop_finalization_pending'] is False
        assert len(list(Path(intent['results_root']).glob('run_vast_*'))) == 1


@pytest.mark.parametrize('terminal', ['completed', 'failed', 'cancelled'])
def test_stop_without_final_data_preserves_terminal_state(job, monkeypatch, terminal):
    """The no-final-data fallback rechecks State and preserves all terminal statuses."""
    import vast_job_runner as runner
    store, identifier, intent = job
    store.update(identifier, stop_rpc_at=1000, final_collection_at=1100)
    monkeypatch.setattr(runner.time, 'time', lambda: 1100)

    class Connection:
        """End the job after Stop's initial read, without final raw results."""
        def collect(self, *args, **kwargs):
            """Simulate a concurrent terminal writer before a failed download."""
            store.update(identifier, status=terminal)
            raise RuntimeError('isolated download failure')

    assert runner.stop_and_collect(Connection(), store, identifier, intent, lease_id='b'*32)
    assert store.read(identifier)['status'] == terminal
    assert not store.read(identifier, 'control.json')['cleanup']


def test_stop_marker_failure_keeps_successful_finalization(job, monkeypatch):
    """A marker write error does not turn successful result publication into a failed job."""
    import vast_job_runner as runner
    store, identifier, intent = job
    final_result_fixture(store, identifier)
    store.update(identifier, stop_rpc_at=1000, final_collection_at=1100)
    monkeypatch.setattr(runner.time, 'time', lambda: 1100)
    update = store.update

    def marker_error(identifier, **changes):
        """Inject only the final marker write failure."""
        if changes.get('stop_results_published'):
            raise OSError('isolated marker write failure')
        return update(identifier, **changes)

    monkeypatch.setattr(store, 'update', marker_error)
    assert runner.stop_and_collect(object(), store, identifier, intent)
    assert store.read(identifier)['status'] == 'completed'
    assert store.read(identifier)['error'] is None


def test_runner_catch_does_not_overwrite_successful_retry(job, monkeypatch):
    """The runner's delayed exception handler logs, cleans up without a lease, and returns."""
    import vast_job_runner as runner
    store, identifier, _ = job
    final_result_fixture(store, identifier)
    original = runner.finalize_collected_results

    def success_then_raise(*args, **kwargs):
        """Model another API retry completing before the runner handles its exception."""
        original(*args, **kwargs)
        raise RuntimeError('earlier finalizer error')

    monkeypatch.setattr(runner, 'finalize_collected_results', success_then_raise)
    runner.run_loop(store, identifier)
    assert store.read(identifier)['status'] == 'completed'
    assert store.read(identifier, 'control.json')['cleanup']


@pytest.mark.parametrize('changed', ['status', 'final_collected', 'stop_reason', 'error', 'finished', 'binary'])
def test_startup_repair_rechecks_after_finalization_lock(job, monkeypatch, changed):
    """A stale startup inventory never repairs evidence changed by a concurrent finalizer."""
    from contextlib import contextmanager
    import vast_job_runner as runner
    store, identifier, _ = job
    final = final_result_fixture(store, identifier)
    binary = final / 'optimize_results/run/all_results.bin'
    binary.write_bytes(b'')
    write_json(final / 'finished.json', {'cancelled': True, 'exit_code': 130})
    store.update(identifier, status='failed', stop_reason='requested', error='Raw results saved locally; result import needs retry')
    original = runner.advisory_file_lock

    @contextmanager
    def intervening_finalizer(target):
        """Change evidence after the inventory snapshot but before lock ownership."""
        if changed == 'finished':
            write_json(final / 'finished.json', {'exit_code': 0})
        elif changed == 'binary':
            binary.write_bytes(b'not empty')
        else:
            values = dict(status='completed', final_collected=False, stop_reason='convergence', error='Optimizer error')
            store.update(identifier, **{changed: values[changed]})
        with original(target):
            yield

    monkeypatch.setattr(runner, 'advisory_file_lock', intervening_finalizer)
    assert runner.recover_cancelled_empty_results(store) == 0


def test_verified_worker_dispatch_releases_state_lock(job, monkeypatch):
    """Worker dispatch may acquire Queue/State only after the rental helper releases State."""
    import file_lock
    import vast_queue
    import vast_job_runner as runner
    store, identifier, _ = job
    store.update(identifier, kind='worker', rental_state='deletion_verified')
    calls = []

    def step(queue, identifier, **kwargs):
        """Assert lock ownership at the actual dispatch boundary."""
        calls.append(identifier)
        assert not getattr(file_lock._HELD_LOCKS, 'entries', {})

    monkeypatch.setattr(vast_queue, 'worker_step', step)
    runner.finalize_verified_rental(store, identifier, now=1100)
    assert calls == [identifier]


@pytest.mark.parametrize('offline', [None, False, True])
def test_cloud_export_always_enforces_offline_without_changing_source(offline):
    """Even an online local config cannot permit remote exchange downloads."""
    source = direction_config()
    if offline is not None:
        source['backtest']['offline'] = offline
    before = copy.deepcopy(source)
    exported = native_job_config(source, 512, 4, False)
    assert exported['backtest']['offline'] is True
    assert source == before


def test_cloud_export_rejects_refetch_before_renting():
    """Offline-incompatible gap refetch fails at the local export boundary."""
    source = direction_config()
    source['backtest']['force_refetch_gaps'] = True
    with pytest.raises(VastError, match='force_refetch_gaps'):
        native_job_config(source, 512, 4, False)


def test_list_skips_job_directory_removed_during_scan(tmp_path, monkeypatch):
    """A concurrent permanent deletion does not fail the complete queue snapshot."""
    store = JobStore(tmp_path / "vast")
    surviving = 'a' * 32
    disappearing = 'b' * 32
    for identifier in (surviving, disappearing):
        directory = ensure_private_directory(store.root / 'jobs' / identifier)
        write_json(directory / 'state.json', {'id': identifier, 'status': 'failed'})
    original_read = store.read

    def read_with_delete(identifier, filename='state.json'):
        """Model a directory disappearing between enumeration and record read."""
        if identifier == disappearing:
            shutil.rmtree(store.root / 'jobs' / identifier)
            raise VastError('Cloud job record cannot be read', 500)
        return original_read(identifier, filename)

    monkeypatch.setattr(store, 'read', read_with_delete)

    assert [row['id'] for row in store.list()] == [surviving]


def test_clone_prepared_reuses_verified_immutable_archive(tmp_path, monkeypatch):
    """An unchanged provider-failed job references its immutable snapshot immediately."""
    store = JobStore(tmp_path / "vast")
    source = store.create_preparation("same", 20_000, 4, False)
    directory = store.directory(source["id"])
    input_directory = ensure_private_directory(directory / "input")
    (input_directory / "optimize.json").write_text("{}")
    shard = input_directory / "ohlcv/binance/BTC/2026-01-01.npy"
    shard.parent.mkdir(parents=True)
    shard.write_bytes(b"immutable candle shard")
    write_json(input_directory / "manifest.json", {
        "schema_version": 1,
        "config_sha256": digest(input_directory / "optimize.json"),
        "files": [{
            "path": "ohlcv/binance/BTC/2026-01-01.npy",
            "bytes": shard.stat().st_size,
            "sha256": digest(shard),
        }],
    })
    archive = directory / "input.tar.gz"
    archive.write_bytes(b"immutable prepared archive")
    archive.chmod(0o600)
    write_json(directory / "intent.json", {
        "id": source["id"], "image": IMAGE, "pb8_revision": REVISION,
        "results_root": str(tmp_path / "old-results"), "bundle_sha256": digest(archive),
        "bundle_bytes": archive.stat().st_size, "source_config_sha256": "same-hash",
    })
    store.update(source["id"], status="failed", use_adg=False, sweep_enabled=True,
                 exchanges=["binance"], input_progress={
                     "stage": "complete", "files_completed": 7, "files_total": 7,
                     "bytes_completed": 1234, "bytes_total": 1234,
                 })
    monkeypatch.setattr("pb8_config.load_pb8_config", lambda _path: {
        "backtest": {"offline": True},
        "optimize": {"iters": 20_000, "n_cpus": 4},
    })

    freeze_test_proof(store, source['id'])
    row = store.clone_prepared(source["id"], "same", "same-hash", tmp_path / "new-results",
                               20_000, 4, False)

    assert row is not None and row["status"] == "ready"
    assert row["requeue_from"] == source["id"]
    assert row["input_progress"] == {
        "stage": "complete", "files_completed": 7, "files_total": 7,
        "bytes_completed": 1234, "bytes_total": 1234,
    }
    target = store.directory(row["id"]) / "input.tar.gz"
    assert target.read_bytes() == archive.read_bytes()
    assert target.stat().st_ino == archive.stat().st_ino
    cloned_config = store.directory(row["id"]) / "input/optimize.json"
    assert cloned_config.read_bytes() == (input_directory / "optimize.json").read_bytes()
    assert digest(cloned_config) == store.read(row["id"], "input/manifest.json")["config_sha256"]
    cloned_shard = store.directory(row["id"]) / "input/ohlcv/binance/BTC/2026-01-01.npy"
    assert not cloned_shard.exists()
    assert store.prepared_input_directory(row["id"]) / "ohlcv/binance/BTC/2026-01-01.npy" == shard
    assert row["snapshot_source_id"] == source["id"]
    assert store.read(row["id"], "intent.json")["results_root"] == str((tmp_path / "new-results").resolve())


def test_create_calibration_reuses_frozen_input_without_mutating_source(tmp_path):
    """Calibration gets independent state while hard-linking immutable input bytes."""
    store = JobStore(tmp_path / 'vast')
    source = store.create_preparation('benchmark', 20_000, 8, False)
    directory = store.directory(source['id'])
    input_directory = ensure_private_directory(directory / 'input')
    config = input_directory / 'optimize.json'
    write_json(config, direction_config())
    shard = input_directory / 'ohlcv/binance/1m/BTC/data.npy'
    shard.parent.mkdir(parents=True)
    shard.write_bytes(b'candles')
    write_json(input_directory / 'manifest.json', {
        'schema_version': 1, 'config_sha256': digest(config),
        'files': [{'path': 'ohlcv/binance/1m/BTC/data.npy', 'bytes': shard.stat().st_size,
                   'sha256': digest(shard)}],
    })
    archive = directory / 'input.tar.gz'
    archive.write_bytes(b'archive')
    archive.chmod(0o600)
    write_json(directory / 'intent.json', {
        'id': source['id'], 'image': IMAGE, 'pb8_revision': REVISION,
        'results_root': str(tmp_path / 'results'), 'bundle_sha256': digest(archive),
        'bundle_bytes': archive.stat().st_size, 'source_config_sha256': 'f' * 64,
    })
    store.update(source['id'], status='ready', input_bytes=archive.stat().st_size,
                 use_adg=False, exchanges=['binance'], input_progress={
                     'stage': 'complete', 'files_completed': 1, 'files_total': 1,
                     'bytes_completed': shard.stat().st_size, 'bytes_total': shard.stat().st_size,
                 })
    with pytest.raises(VastError, match='Invalid GPU calibration plan'):
        store.create_calibration(source['id'], {
            'protocol': 1, 'populations': [1024, 2048, 4096, 8192],
            'sample_seconds': 300, 'allow_32768': False,
        })
    from vast_calibration import configurable_calibration_plan
    freeze_test_proof(store, source['id'])
    plan = configurable_calibration_plan(5632, 512, 7168, .05, 300, 3600)
    row = store.create_calibration(source['id'], plan)
    target = store.directory(row['id'])
    assert row['kind'] == 'calibration' and row['status'] == 'ready'
    assert row['calibration_source_id'] == source['id']
    assert row['calibration_plan'] == plan
    assert (target / 'input.tar.gz').stat().st_ino == archive.stat().st_ino
    assert (target / 'input/ohlcv/binance/1m/BTC/data.npy').stat().st_ino == shard.stat().st_ino
    assert store.read(source['id'])['status'] == 'ready'


def test_clone_prepared_rejects_changed_config_without_creating_job(tmp_path, monkeypatch):
    """Snapshot reuse falls back before creating state when the saved config changed."""
    store = JobStore(tmp_path / "vast")
    source = store.create_preparation("same", 20_000, 4, False)
    directory = store.directory(source["id"])
    input_directory = ensure_private_directory(directory / "input")
    (input_directory / "optimize.json").write_text("{}")
    write_json(input_directory / "manifest.json", {"files": []})
    archive = directory / "input.tar.gz"
    archive.write_bytes(b"archive")
    write_json(directory / "intent.json", {
        "id": source["id"], "image": IMAGE, "pb8_revision": REVISION,
        "bundle_sha256": digest(archive), "bundle_bytes": archive.stat().st_size,
        "source_config_sha256": "old-hash",
    })
    store.update(source["id"], status="failed", use_adg=False,
                 input_progress={"stage": "complete"})
    monkeypatch.setattr("pb8_config.load_pb8_config", lambda _path: {
        "optimize": {"iters": 20_000, "n_cpus": 4},
    })
    before = {path.name for path in (store.root / "jobs").iterdir()}

    assert store.clone_prepared(source["id"], "same", "new-hash", tmp_path / "results",
                                20_000, 4, False) is None
    assert {path.name for path in (store.root / "jobs").iterdir()} == before


@pytest.mark.parametrize('reason,improved', [('rental_deadline', False), ('requested', True), ('convergence', True)])
def test_final_snapshot_checked_without_rewriting_stop_reason(job, monkeypatch, reason, improved):
    """Final import uses its count, preserves stop history, and is retry-idempotent."""
    from vast_convergence import DEFAULTS, advance
    import vast_job_runner as runner
    store, identifier, _ = job
    config = {**DEFAULTS, 'convergence_enabled': True}
    history = advance({}, [[1, 1]], 512, config)
    if reason == 'convergence':
        history = advance(history, [[1, 1]], 1024, config)
    final = store.directory(identifier) / 'final-results'
    front = final / 'optimize_results/run/pareto'
    front.mkdir(parents=True)
    write_json(front / 'point.json', {'metrics': {
        'constraint_violation': 0, 'unpenalized_objectives': [0.9, 0.9] if improved else [1, 1]}})
    write_json(final / 'finished.json', {'cancelled': True, 'exit_code': 130, 'wall_seconds': 120})
    store.update(identifier, final_collected=True, exact_completed=1098,
                 convergence=history, convergence_config=config, stop_reason=reason)
    monkeypatch.setattr(runner, 'import_results', lambda *args: {'evaluations': 1100, 'result_path': 'mock'})
    row = runner.finalize_collected_results(store, identifier)
    assert row['exact_completed'] == 1100
    assert row['completion_reason'] == reason
    assert row['status'] == ('cancelled' if reason == 'requested' else 'completed')
    assessment = row['convergence'].pop('final')
    assert row['convergence'] == history
    assert assessment['checked_exact'] == 1100
    assert assessment['threshold_reached'] is (not improved)
    assert assessment['last_improvement_exact'] == (1100 if improved else 512)
    assert not store.read(identifier, 'control.json')['stop']
    again = runner.finalize_collected_results(store, identifier)
    assert again['convergence']['final'] == assessment
    assert again['completion_reason'] == reason


def test_invalid_final_convergence_keeps_results_and_reports_unavailable(job, monkeypatch):
    """A damaged quality snapshot cannot turn a successful import into a stop."""
    from vast_convergence import DEFAULTS
    import vast_job_runner as runner
    store, identifier, _ = job
    final = store.directory(identifier) / 'final-results'
    front = final / 'optimize_results/run/pareto'
    front.mkdir(parents=True)
    (front / 'point.json').write_text('{broken')
    write_json(final / 'finished.json', {'exit_code': 0})
    store.update(identifier, convergence_config={**DEFAULTS, 'convergence_enabled': True})
    monkeypatch.setattr('vast_convergence._log', lambda *args, **kwargs: None)
    monkeypatch.setattr(runner, 'import_results', lambda *args: {'evaluations': 1100, 'result_path': 'mock'})
    row = runner.finalize_collected_results(store, identifier)
    assert row['status'] == 'completed'
    assert row['result_path'] == 'mock'
    assert row['convergence']['final']['phase'] == 'waiting'
    assert 'checked_exact' not in row['convergence']['final']
    assert not store.read(identifier, 'control.json')['stop']


def test_requested_stop_with_empty_result_binary_is_cancelled(job, monkeypatch):
    """A stop before the first exact result is not misreported as an import failure."""
    import vast_job_runner as runner
    store, identifier, _ = job
    final = store.directory(identifier) / 'final-results'
    result = final / 'optimize_results/run'
    result.mkdir(parents=True)
    (result / 'all_results.bin').write_bytes(b'')
    write_json(final / 'finished.json', {
        'exit_code': -15, 'cancelled': True, 'wall_seconds': 120, 'finished_at': 1234,
    })
    store.update(identifier, final_collected=True, stop_reason='requested')
    monkeypatch.setattr(runner, 'import_results', lambda *args: pytest.fail('empty result must not be imported'))

    row = runner.finalize_collected_results(store, identifier)

    assert row['status'] == 'cancelled'
    assert row['error'] is None
    assert row['exit_code'] == -15
    assert row['completion_reason'] == 'requested'
    assert row['elapsed_seconds'] == 120


def test_startup_recovery_repairs_legacy_empty_result_failure(job):
    """Startup migration repairs a previously persisted false import failure."""
    import vast_job_runner as runner
    store, identifier, _ = job
    final = store.directory(identifier) / 'final-results'
    result = final / 'optimize_results/run'
    result.mkdir(parents=True)
    (result / 'all_results.bin').write_bytes(b'')
    write_json(final / 'finished.json', {
        'exit_code': -15, 'cancelled': True, 'wall_seconds': 120, 'finished_at': 1234,
    })
    store.update(identifier, status='failed', final_collected=True, stop_reason='requested',
                 error='Raw results saved locally; result import needs retry')

    assert runner.recover_cancelled_empty_results(store) == 1
    assert runner.recover_cancelled_empty_results(store) == 0
    row = store.read(identifier)
    assert row['status'] == 'cancelled'
    assert row['error'] is None
    assert row['completion_reason'] == 'requested'
    assert row['exit_code'] == -15


@pytest.fixture
def job(tmp_path):
    """Create an authorized fake job without touching real account state."""
    store = JobStore(tmp_path / 'vast')
    identifier = 'a' * 32
    directory = ensure_private_directory(store.root / 'jobs' / identifier)
    intent = {'id': identifier, 'image': IMAGE, 'pb8_revision': REVISION,
              'label': 'pbgui-vast-' + identifier, 'accepted_at': 1000, 'deadline': 2000,
              'budget_usd': 1, 'offer': {'id': 42, 'disk_gb': 40},
              'results_root': str(tmp_path / 'pb8' / 'optimize_results'), 'source_config_sha256': 'source'}
    write_json(directory / 'intent.json', intent)
    write_json(directory / 'state.json', {'id': identifier, 'status': 'provisioning', 'rental_state': 'creation_pending'})
    write_json(directory / 'control.json', {'stop': False, 'cleanup': False})
    return store, identifier, intent


def test_create_preparation_publishes_pending_job_before_snapshot_work(tmp_path):
    """A cloud job is visible immediately while its immutable input is prepared."""
    store = JobStore(tmp_path / 'vast')
    state = store.create_preparation('cloud-test', 512, 4, False)
    assert state['status'] == 'preparing'
    assert state['config_name'] == 'cloud-test'
    assert state['input_progress']['stage'] == 'selecting'
    assert store.read(state['id'], 'control.json') == {'stop': False, 'cleanup': False}


def test_large_input_manifest_keeps_control_record_limit(tmp_path):
    """Large shard manifests remain readable without widening mutable state records."""
    store = JobStore(tmp_path / 'vast')
    identifier = 'a' * 32
    directory = ensure_private_directory(store.root / 'jobs' / identifier)
    ensure_private_directory(directory / 'input')
    manifest_padding = 'x' * (65 * 1024 * 1024)
    write_json(directory / 'input/manifest.json', {'files': [], 'padding': manifest_padding})
    assert len(store.read(identifier, 'input/manifest.json')['padding']) == len(manifest_padding)
    control_padding = 'x' * (17 * 1024 * 1024)
    write_json(directory / 'state.json', {'id': identifier, 'padding': control_padding})
    with pytest.raises(VastError, match='record cannot be read'):
        store.read(identifier)


def test_input_archive_reports_consumed_bytes_and_eta(tmp_path):
    """Compression progress follows bytes actually read and produces the same input layout."""
    source = ensure_private_directory(tmp_path / 'input')
    ensure_private_directory(source / 'ohlcv/binance')
    (source / 'optimize.json').write_text('{}')
    (source / 'manifest.json').write_text('{"files":[]}')
    payload = b'candles' * 200_000
    (source / 'ohlcv/binance/BTC.npy').write_bytes(payload)
    samples = []
    target = tmp_path / 'input.tar.gz'
    _write_input_archive(source, [Path('binance/BTC.npy')], target, samples.append)
    assert samples[0]['bytes_completed'] == 0
    assert samples[-1]['bytes_completed'] == samples[-1]['bytes_total']
    assert samples[-1]['files_completed'] == samples[-1]['files_total'] == 3
    assert samples[-1]['bytes_per_second'] > 0
    assert samples[-1]['eta_seconds'] == 0
    with tarfile.open(target, 'r:gz') as archive:
        assert archive.extractfile('input/ohlcv/binance/BTC.npy').read() == payload


class Provider:
    """A provider double with ambiguous create and observable mutation calls."""
    def __init__(self, rows=(), fail=False):
        """Keep the simulated account and request history local."""
        self.rows, self.fail, self.calls = list(rows), fail, []

    def instances(self, *, fresh=False):
        """Return only fake instances."""
        return self.rows

    def request(self, method, path, body=None):
        """Record paid mutations and optionally simulate a lost create reply."""
        self.calls.append((method, path))
        if self.fail:
            raise VastError('Ambiguous timeout')
        return {'new_contract': 123}


def test_ambiguous_create_is_not_retried(job):
    """A lost paid response must never trigger a second rental."""
    store, identifier, intent = job
    client = Provider(fail=True)
    with pytest.raises(VastError):
        guard_step(store, identifier, client, intent, 'fake_token', now=1100)
    client.fail = False
    guard_step(store, identifier, client, intent, 'fake_token', now=1101)
    assert client.calls == [('PUT', '/asks/42/')]


def test_guard_deletes_only_owned_instance_and_verifies_absence(job):
    """Deadline deletion is limited to the full label and requires two checks."""
    store, identifier, intent = job
    client = Provider([{'id': 123, 'label': intent['label']}, {'id': 999, 'label': 'foreign'}])
    assert not guard_step(store, identifier, client, intent, 'fake_token', now=2001)
    assert client.calls == [('DELETE', '/instances/123/')]
    client.rows = [{'id': 999, 'label': 'foreign'}]
    assert not guard_step(store, identifier, client, intent, 'fake_token', now=2010)
    assert guard_step(store, identifier, client, intent, 'fake_token', now=2020)
    assert store.read(identifier)['rental_state'] == 'deletion_verified'
    assert store.read(identifier)['provider_status'] == 'deleted'


def test_worker_guard_finishes_despite_unreadable_foreign_job(job):
    """Provider deletion and local finalization never enumerate foreign corrupt state strictly."""
    store, identifier, intent = job
    store.update(identifier, kind='worker', deadline=2000)
    (store.root / 'jobs' / ('b' * 32)).mkdir()
    client = Provider([{'id': 123, 'label': intent['label']}])
    assert not guard_step(store, identifier, client, intent, '', now=2001)
    assert client.calls == [('DELETE', '/instances/123/')]
    client.rows = []
    assert not guard_step(store, identifier, client, intent, '', now=2010)
    assert guard_step(store, identifier, client, intent, '', now=2020)
    assert store.read(identifier)['rental_state'] == 'deletion_verified'
    assert store.read(identifier)['status'] == 'completed'
    assert store.read(identifier)['inventory_error']


def test_guard_spaces_accepted_deletes_without_delaying_absence_checks(job):
    """Provider propagation never causes repeated DELETEs every guard tick."""
    store, identifier, intent = job
    client = Provider([{'id': 123, 'label': intent['label']}])
    for now in (2001, 2011, 2021, 2051):
        assert not guard_step(store, identifier, client, intent, '', now=now)
    assert client.calls == [('DELETE', '/instances/123/')]
    assert not guard_step(store, identifier, client, intent, '', now=2061)
    assert len(client.calls) == 2
    client.rows = []
    assert not guard_step(store, identifier, client, intent, '', now=2062)
    assert guard_step(store, identifier, client, intent, '', now=2072)
    assert store.read(identifier)['rental_state'] == 'deletion_verified'


def test_stop_before_create_prevents_rental(job):
    """A stop arriving during provisioning cannot create a new paid instance."""
    store, identifier, intent = job
    store.control(identifier, 'stop')
    client = Provider()
    guard_step(store, identifier, client, intent, 'fake_token', now=1100)
    assert client.calls == []
    run_loop(store, identifier)
    assert store.read(identifier)['status'] == 'cancelled'
    assert store.read(identifier, 'control.json')['cleanup']


def test_instance_id_mismatch_is_not_deleted(job):
    """A reused label cannot authorize deleting a different stored identity."""
    store, identifier, intent = job
    store.update(identifier, instance_id=555)
    client = Provider([{'id': 123, 'label': intent['label']}])
    with pytest.raises(VastError, match='ID does not match'):
        guard_step(store, identifier, client, intent, 'fake_token', now=2001)
    assert not client.calls


@pytest.mark.parametrize('path', ['../x', '/tmp/x', 'x/../y', 'x//y', 'x\\y', 'x\x00y'])
def test_transfer_path_rejects_traversal(tmp_path, path):
    """Remote filenames cannot escape a local staging directory."""
    with pytest.raises(ValueError):
        safe_path(tmp_path, path)


@pytest.mark.parametrize('url', ['http://s3.amazonaws.com/vast.ai/instance_logs/a', 'https://127.0.0.1/x',
                                'https://s3.amazonaws.com/another-bucket/a', 'https://evil.test/instance_logs/x'])
def test_host_identity_rejects_untrusted_urls(url):
    """Provider responses cannot direct SSH bootstrap at arbitrary HTTP targets."""
    with pytest.raises(VastError):
        fetch_host_key_result(url)


@pytest.mark.parametrize('matching', [True, False])
def test_host_identity_is_bound_to_rental(monkeypatch, matching):
    """Accept a public key only from the requested rental's provider log marker."""
    import base64
    from types import SimpleNamespace
    import vast_transfer
    key = 'ssh-ed25519 ' + base64.b64encode(b'\x00\x00\x00\x0bssh-ed25519\x00\x00\x00\x20' + b'x'*32).decode()
    output = 'PBGUI_HOST_KEY ' + ('a' if matching else 'b')*32 + ' ' + key + '\n'
    monkeypatch.setattr(vast_transfer.urllib.request, 'build_opener',
                        lambda *_: SimpleNamespace(open=lambda *a, **kw: io.BytesIO(output.encode())))
    url = 'https://s3.amazonaws.com/public.vast.ai/instance_logs/example.log'
    if matching:
        assert fetch_host_key_result(url, lease_id='a'*32) == key
    else:
        with pytest.raises(VastError, match='rental'):
            fetch_host_key_result(url, lease_id='a'*32)


def test_slow_image_loading_does_not_consume_worker_setup_timeout(job, monkeypatch):
    """Setup receives its own timer after running, without changing the paid deadline."""
    from types import SimpleNamespace
    import vast_job_runner as runner
    import vast_provisioning_log
    store, identifier, intent = job
    intent['deadline'] = 5000
    write_json(store.directory(identifier)/'intent.json', intent)
    clock = [2001.0]
    monkeypatch.setattr(runner.time, 'time', lambda: clock[0])
    monkeypatch.setattr(runner.time, 'sleep', lambda _: clock.__setitem__(0, clock[0]+5))
    monkeypatch.setattr(runner, 'VastCredentialStore', lambda _: SimpleNamespace(secrets=lambda: {'api_key':'fake'}))
    monkeypatch.setattr(runner, 'VastClient', lambda _: object())
    rows = iter([{'id':123, 'actual_status':'loading'}, {'id':123, 'actual_status':'running'}])
    monkeypatch.setattr(runner, 'owned_instance', lambda *_: next(rows))
    provisioning_log_calls = []
    monkeypatch.setattr(vast_provisioning_log, 'collect_provisioning_log',
                        lambda *args: provisioning_log_calls.append(args))
    class ReachedBootstrap(BaseException):
        """Exit the simulated controller at the newly reached SSH step."""
    def bootstrap(_):
        """Stop before any remote operation."""
        raise ReachedBootstrap()
    monkeypatch.setattr(runner, 'WorkerConnection', lambda *a, **kw: SimpleNamespace(bootstrap=bootstrap))
    with pytest.raises(ReachedBootstrap):
        run_loop(store, identifier)
    assert store.read(identifier)['setup_started_at'] == 2006
    assert store.read(identifier)['status'] == 'provisioning'
    assert store.read(identifier, 'intent.json')['deadline'] == 5000
    assert len(provisioning_log_calls) == 1
    assert provisioning_log_calls[0][3] == 123


def test_worker_setup_timeout_still_applies_after_running(job, monkeypatch):
    """A stalled SSH/upload setup remains bounded once the container is running."""
    import vast_job_runner as runner
    store, identifier, _ = job
    store.update(identifier, setup_started_at=1000)
    monkeypatch.setattr(runner.time, 'time', lambda: 1901)
    run_loop(store, identifier)
    assert store.read(identifier)['error'] == 'Worker setup exceeded 15 minutes'
    assert store.read(identifier, 'control.json')['cleanup']


def test_bootstrap_uses_container_logs_and_retains_verified_key(job, monkeypatch):
    """Avoid unsupported Execute and stop provider requests after pinning the key."""
    from types import SimpleNamespace
    import vast_transfer
    store, identifier, _ = job
    directory = store.directory(identifier)
    (directory/'ssh-key').write_text('synthetic private key')
    (directory/'ssh-key.pub').write_text('synthetic public key')
    write_json(directory/'ssh-attached.json', {'instance_id':123})
    calls = []
    def request(method, path, body):
        """Return only the fake provider log location."""
        calls.append((method, path, body))
        return {'result_url':'https://s3.amazonaws.com/public.vast.ai/instance_logs/identity.log'}
    fetches = []
    def fetch(url, *, lease_id):
        """Simulate one upload delay followed by authenticated key retrieval."""
        fetches.append((url, lease_id))
        if len(fetches) == 1:
            raise VastError('SSH host identity is not available yet')
        return 'ssh-ed25519 synthetic'
    monkeypatch.setattr(vast_transfer, 'fetch_host_key_result', fetch)
    connection = vast_transfer.WorkerConnection(store, identifier,
        {'id':123,'public_ipaddr':'8.8.8.8','ports':{'22/tcp':[{'HostPort':'2222'}]}})
    client = SimpleNamespace(request=request)
    with pytest.raises(VastError, match='not available'):
        connection.bootstrap(client)
    connection.bootstrap(client)
    connection.bootstrap(client)
    assert len(calls) == 1
    assert calls[0] == ('PUT', '/instances/request_logs/123/',
                       {'tail':'1000'})
    assert len(fetches) == 2
    assert (directory/'known_hosts').read_text() == f'pbgui-{identifier} ssh-ed25519 synthetic\n'


def test_result_tar_symlinks_rejected(tmp_path):
    """Archives cannot write through symlinks into unrelated local data."""
    path = tmp_path / 'bad.tar.gz'
    with tarfile.open(path, 'w:gz') as archive:
        entry = tarfile.TarInfo('optimizer.log'); entry.type = tarfile.SYMTYPE; entry.linkname = '/tmp/other'
        archive.addfile(entry)
    with pytest.raises(VastError):
        extract_results(path, tmp_path, 'final')


def test_native_import_is_idempotent_and_restores_sweep(job):
    """Import native records once and retain PBGui sweep metadata."""
    store, identifier, intent = job
    directory = store.directory(identifier)
    native = directory / 'final-results/optimize_results/test_run'
    native.mkdir(parents=True)
    (native / 'all_results.bin').write_bytes(msgpack.packb({'config': {}, 'metrics': {}}))
    (native / 'pareto').mkdir()
    (native / 'pareto/a.json').write_text('{}')
    (directory / 'input').mkdir()
    write_json(directory / 'input/manifest.json', {'sweep_plan': {'enabled': True}})
    first = import_results(store, identifier)
    assert first == import_results(store, identifier)
    assert first['evaluations'] == 1
    from pathlib import Path
    assert json.loads((Path(first['result_path']) / '.pbgui_sweep_cycles.json').read_text()) == {'enabled': True}
    assert len(list(Path(intent['results_root']).glob('*_vast_*'))) == 1


def test_incomplete_native_binary_is_not_imported(job):
    """A truncated final record cannot be exposed as a completed optimizer run."""
    store, identifier, _ = job
    native = store.directory(identifier) / 'final-results/optimize_results/test_run'
    native.mkdir(parents=True)
    (native / 'all_results.bin').write_bytes(msgpack.packb({'ok': 1}) + b'\x81')
    with pytest.raises(VastError, match='incomplete'):
        import_results(store, identifier)


def test_job_objective_change_is_explicit_and_source_is_immutable():
    """ADG conversion affects only the authorized copy and retains source metadata."""
    source = {'live': {'strategy_kind':'ema_anchor', 'approved_coins':{'long':['BTC'], 'short':[]}},
              'bot': {'long':{}, 'short':{}}, 'backtest': {'exchanges':['binance']}, 'pbgui': {'sweep': 'keep'},
              'optimize': {'scoring':[{'metric':'gain_strategy_eq'}], 'limits':[]}}
    source = complete_direction(source)
    original = copy.deepcopy(source)
    with pytest.raises(VastError, match='Unsupported cloud metrics'):
        native_job_config(source, 512, 4, False)
    result = native_job_config(source, 512, 4, True)
    assert result['optimize']['scoring'][0]['metric'] == 'adg_strategy_eq'
    assert result['optimize']['backend'] == 'gpu'
    assert source == original
    assert 'pbgui' not in result


def test_worker_snapshot_round_trip(job, monkeypatch):
    """A worker-produced final archive verifies and imports through the controller."""
    from setup.vast_gpu_benchmark import cloud_worker
    store, identifier, intent = job
    directory = store.directory(identifier)
    remote = directory / 'fake-worker'
    output = remote / 'output/optimize_results/2026_test'
    output.mkdir(parents=True)
    (output / 'pareto').mkdir()
    (output / 'pareto/a.json').write_text('{}')
    (output / 'all_results.bin').write_bytes(msgpack.packb({'config': {}, 'metrics': {}}))
    (output / 'checkpoint.pkl').write_bytes(b'untrusted-pickle-must-not-be-auto-loaded')
    (remote / 'output/optimizer.log').write_text('Iter: 512\n')
    write_json(remote / 'finished.json', {'exit_code': 0})
    monkeypatch.setattr(cloud_worker, 'ROOT', remote)
    metadata = cloud_worker.snapshot(True)
    assert metadata['sha256'] == digest(remote / 'final.tar.gz')
    extract_results(remote / 'final.tar.gz', directory, 'final')
    (directory / 'input').mkdir()
    write_json(directory / 'input/manifest.json', {'sweep_plan': None})
    result = import_results(store, identifier)
    from pathlib import Path
    assert result['evaluations'] == 1
    assert not (Path(result['result_path']) / 'checkpoint.pkl').exists()
    assert (directory / 'final-results/optimize_results/2026_test/checkpoint.pkl').exists()


def test_resume_existing_supervisor_does_not_launch_duplicate(job, monkeypatch):
    """An active systemd unit is reused on recovery requests."""
    import vast_jobs
    from types import SimpleNamespace
    store, identifier, _ = job
    calls = []
    monkeypatch.setattr(vast_jobs, 'PROJECT', store.root)
    def fake_run(args, **kwargs):
        """Simulate an already running supervisor without launching a process."""
        calls.append(args)
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(vast_jobs.subprocess, 'run', fake_run)
    store.launch_service(identifier, 'guard')
    assert len(calls) == 1
    assert calls[0][:4] == ['systemctl', '--user', 'is-active', '--quiet']


def test_multicoin_native_job_preserves_both_sides():
    """Export multiple coins without narrowing the portfolio or modifying the source."""
    source = {'live': {'strategy_kind':'ema_anchor', 'approved_coins':{'long':['BTC','ETH'], 'short':['ETH','SOL']}},
              'bot': {'long':{}, 'short':{}}, 'backtest': {'exchanges':['binance']},
              'optimize': {'scoring':[{'metric':'adg_strategy_eq'}], 'limits':[]}}
    source = complete_direction(source)
    original = copy.deepcopy(source)
    result = native_job_config(source, 512, 4, False)
    assert result['live']['approved_coins'] == original['live']['approved_coins']
    assert result['optimize']['backend'] == 'gpu'
    assert source == original


@pytest.mark.parametrize('sent', [False, True])
def test_rate_limit_preserves_only_actual_create_attempts(job, sent):
    """Local cooldown cannot mark an unsent rental as an ambiguous paid request."""
    from vast_provider import VastRateLimit
    store, identifier, intent = job
    class Limited(Provider):
        """Simulate either local deferral or upstream 429."""
        def request(self, method, path, body=None):
            """Raise without contacting a real provider."""
            raise VastRateLimit(30, request_sent=sent)
    with pytest.raises(VastRateLimit):
        guard_step(store, identifier, Limited(), intent, '', now=1100)
    assert (store.directory(identifier) / 'attempt.json').exists() is sent
    assert bool(store.read(identifier).get('creation_error')) is sent


def test_ambiguous_creation_cleanup_uses_two_fresh_checks_after_grace(job):
    """Release an absent rental before its full lease deadline, never on errors."""
    store, identifier, intent = job
    client = Provider(fail=True)
    with pytest.raises(VastError):
        guard_step(store, identifier, client, intent, '', now=1100)
    store.control(identifier, 'cleanup')
    client.fail = False
    assert not guard_step(store, identifier, client, intent, '', now=1150)
    assert store.read(identifier)['cleanup_wait_until'] == 1220
    assert not guard_step(store, identifier, client, intent, '', now=1221)
    assert store.read(identifier)['rental_state'] == 'destroy_pending'
    assert guard_step(store, identifier, client, intent, '', now=1231)
    assert store.read(identifier)['rental_state'] == 'deletion_verified'
    assert client.calls == [('PUT', '/asks/42/')]


def test_rejected_creation_is_not_reported_as_disappeared_instance(job):
    """A failed create followed by confirmed absence reports that no rental existed."""
    store, identifier, intent = job
    client = Provider(fail=True)
    with pytest.raises(VastError, match='Ambiguous timeout'):
        guard_step(store, identifier, client, intent, '', now=1100)
    client.fail = False
    assert not guard_step(store, identifier, client, intent, '', now=1221)
    assert guard_step(store, identifier, client, intent, '', now=1231)
    state = store.read(identifier)
    assert state['rental_end_reason'] == 'provider_creation_failed'
    assert state['status'] == 'failed'
    assert 'Rental was not created: Ambiguous timeout' in state['error']
    assert 'select another host' in state['error']


def test_host_identity_accepts_legacy_timestamp_split(monkeypatch):
    """An injected timestamp may split the rental marker from its public key."""
    import base64
    from types import SimpleNamespace
    import vast_transfer
    key = 'ssh-ed25519 ' + base64.b64encode(b'\x00\x00\x00\x0bssh-ed25519\x00\x00\x00\x20' + b'y'*32).decode()
    output = 'PBGUI_HOST_KEY ' + 'a'*32 + ' Sun Sep 13 19:43:39 UTC 2026\n' + key + '\n'
    monkeypatch.setattr(vast_transfer.urllib.request, 'build_opener',
                        lambda *_: SimpleNamespace(open=lambda *a, **kw: io.BytesIO(output.encode())))
    assert fetch_host_key_result('https://s3.amazonaws.com/public.vast.ai/instance_logs/test.log', lease_id='a'*32) == key


def test_remote_commands_use_pinned_checkout(job, monkeypatch):
    """Rust preflight and optimizer startup must resolve the same source tree."""
    import vast_transfer
    store, identifier, _ = job
    connection = vast_transfer.WorkerConnection(store, identifier,
        {'id':123,'public_ipaddr':'8.8.8.8','ports':{'22/tcp':[{'HostPort':'2222'}]}})
    calls = []
    def spawn(args, **kwargs):
        """Capture argv without starting a process or opening SSH."""
        calls.append(args)
        raise OSError('synthetic')
    monkeypatch.setattr(vast_transfer.subprocess, 'Popen', spawn)
    with pytest.raises(VastError):
        connection.operation('health')
    assert calls[0][-1].startswith('cd /opt/passivbot && PBGUI_WORKDIR=')
    assert 'StrictHostKeyChecking=yes' in calls[0]


def test_ssh_progress_runs_without_remote_stdout(job, monkeypatch):
    """A silent upload reports consumed input and still reaps its subprocess."""
    import sys
    import subprocess
    import vast_transfer
    store, identifier, _ = job
    connection = vast_transfer.WorkerConnection(store, identifier,
        {'id':123,'public_ipaddr':'8.8.8.8','ports':{'22/tcp':[{'HostPort':'2222'}]}})
    source_path = store.directory(identifier)/'synthetic-upload.bin'
    source_path.write_bytes(b'x'*8192)
    real_popen = subprocess.Popen
    def spawn(args, **kwargs):
        """Replace SSH with an isolated local stdin consumer."""
        return real_popen([sys.executable,'-c','import os,time; os.read(0,1024); time.sleep(.3); os.read(0,10000)'], **kwargs)
    monkeypatch.setattr(vast_transfer.subprocess,'Popen',spawn)
    seen = []
    with source_path.open('rb') as source:
        connection.command('synthetic upload',stdin=source,progress=lambda:seen.append(source.tell()),timeout=5)
    assert len(seen) >= 2
    assert seen[-1] == 8192


@pytest.mark.parametrize('age,valid', [(60, True), (86401, False)])
def test_start_stages_public_cache_with_original_freshness(job, monkeypatch, age, valid):
    """Only fresh verified market snapshots reach the optimizer working directory."""
    import time
    import shlex
    from vast_transfer import WorkerConnection
    store, identifier, _ = job
    root = store.directory(identifier)
    source = ensure_private_directory(root / 'input/caches/binance') / 'markets.json'
    source.write_text('{"public":"market"}')
    stamp = time.time() - age
    write_json(root / 'input/manifest.json', {'files': [{'path':'caches/binance/markets.json','sha256':digest(source)}],
        'public_market_cache_mtimes': {'binance':stamp}})
    connection = WorkerConnection(store, identifier,
        {'id':123,'public_ipaddr':'8.8.8.8','ports':{'22/tcp':[{'HostPort':'2222'}]}})
    connection.remote_root = str(root)
    calls = []
    def command(value, **kwargs):
        """Execute only the cache staging program in the isolated fixture directory."""
        calls.append(value)
        if value.startswith('/usr/local/bin/python -c '):
            exec(shlex.split(value)[2], {})
        return b''
    monkeypatch.setattr(connection, 'command', command)
    monkeypatch.setattr('vast_inception.stage_inception', lambda connection: None)
    if not valid:
        with pytest.raises(VastError, match='expired'):
            connection.start()
        assert calls == []
        return
    connection.start()
    target = root / 'output/caches/binance/markets.json'
    assert target.read_bytes() == source.read_bytes()
    assert abs(target.stat().st_mtime-stamp) < .001
    assert len(calls) == 2


def test_partial_results_refresh_and_finalize_in_one_directory(job):
    """Publish live Pareto changes without duplicates or blocking final import."""
    from pathlib import Path
    import shutil
    store, identifier, intent = job
    directory = store.directory(identifier)
    native = directory / 'partial-results/optimize_results/live_run'
    native.mkdir(parents=True)
    (native / 'pareto').mkdir()
    (native / 'pareto/old.json').write_text('{}')
    record = msgpack.packb({'config': {}, 'metrics': {}})
    (native / 'all_results.bin').write_bytes(record)
    (directory / 'input').mkdir()
    write_json(directory / 'input/manifest.json', {})
    first = import_results(store, identifier, partial=True)
    target = Path(first['result_path'])
    (native / 'all_results.bin').write_bytes(record * 2)
    (native / 'pareto/old.json').unlink()
    (native / 'pareto/new.json').write_text('{}')
    second = import_results(store, identifier, partial=True)
    assert second['result_path'] == first['result_path']
    assert second['evaluations'] == 2
    assert not (target / 'pareto/old.json').exists()
    assert (target / 'pareto/new.json').exists()
    (native / 'all_results.bin').write_bytes(record * 2 + b'\x81')
    with pytest.raises(VastError, match='incomplete'):
        import_results(store, identifier, partial=True)
    assert (target / 'all_results.bin').read_bytes() == record * 2
    (native / 'all_results.bin').write_bytes(record * 3)
    shutil.copytree(native, directory / 'final-results/optimize_results/live_run')
    final = import_results(store, identifier)
    assert final['result_path'] == first['result_path']
    assert final['evaluations'] == 3
    assert not json.loads((target / '.pbgui_vast.json').read_text())['partial']
    assert final == import_results(store, identifier)
    assert len(list(Path(intent['results_root']).glob('*_vast_*'))) == 1


def test_pareto_only_live_snapshot_is_published(job):
    """The lightweight worker backup is browsable without fabricated history."""
    from pathlib import Path
    store, identifier, _ = job
    directory = store.directory(identifier)
    native = directory / 'partial-results/optimize_results/live_run/pareto'
    native.mkdir(parents=True)
    (native / 'candidate.json').write_text('{}')
    (directory / 'input').mkdir()
    write_json(directory / 'input/manifest.json', {})
    store.update(identifier, exact_completed=31)
    result = import_results(store, identifier, partial=True)
    assert result['evaluations'] == 31
    assert result['pareto_count'] == 1
    assert not (Path(result['result_path']) / 'all_results.bin').exists()
    assert import_results(store, identifier, partial=True) == result


@pytest.mark.parametrize('exit_code,expected', [(0, 'completed'), (-2, 'completed'), (1, 'cancelled')])
def test_convergence_completion_keeps_final_results_and_reason(job, monkeypatch, exit_code, expected):
    """Only a successful graceful stop is labeled as convergence completion."""
    import vast_job_runner as runner
    store, identifier, _ = job
    finished = store.directory(identifier) / 'final-results'
    finished.mkdir()
    write_json(finished / 'finished.json', {'exit_code': exit_code, 'cancelled': True})
    store.update(identifier, final_collected=True, convergence={'stop_requested': True})
    monkeypatch.setattr(runner, 'import_results', lambda *args: {'result_path': 'mock-results'})
    runner.run_loop(store, identifier)
    row = store.read(identifier)
    assert row['status'] == expected
    assert row['completion_reason'] == ('convergence' if expected == 'completed' else None)
    assert row['result_path'] == 'mock-results'


@pytest.mark.parametrize('value,valid', [(1789331000.5, True), (None, False), (True, False), (-1, False), ('yesterday', False)])
def test_optimizer_start_time_reads_worker_record(value, valid):
    """Elapsed uses the actual optimizer start, not rental or reconnection time."""
    from types import SimpleNamespace
    from vast_job_runner import optimizer_started_at
    calls = []
    def command(cmd, **kwargs):
        """Return an isolated bounded start record."""
        calls.append((cmd, kwargs))
        return json.dumps({'started_at': value}).encode()
    connection = SimpleNamespace(command=command, remote_root='/work/pbgui/jobs/test')
    if valid:
        assert optimizer_started_at(connection) == value
    else:
        with pytest.raises(VastError, match='start time unavailable'):
            optimizer_started_at(connection)
    assert calls == [('head -c 512 /work/pbgui/jobs/test/started.json', {'max_output': 512})]


def test_native_import_retains_distributed_validation_windows(job):
    """Vast result collection publishes explicit Holdouts for subsequent Validate actions."""
    from pathlib import Path
    from scenario_templates import generate_scenario_template
    from scenario_windows import validation_holdouts
    store, identifier, _ = job
    directory = store.directory(identifier)
    native = directory / 'final-results/optimize_results/window_run'
    native.mkdir(parents=True)
    (native / 'all_results.bin').write_bytes(msgpack.packb({'config': {}, 'metrics': {}}))
    (native / 'pareto').mkdir()
    (native / 'pareto/a.json').write_text('{}')
    (directory / 'input').mkdir()
    plan = generate_scenario_template({'start_date':'2024-01-01','end_date':'2024-12-31','windows':[
        {'id':'a','label':'a','role':'training','start_date':'2024-01-01','end_date':'2024-03-31'},
        {'id':'h','label':'h','role':'holdout','start_date':'2024-04-01','end_date':'2024-04-30'},
        {'id':'b','label':'b','role':'training','start_date':'2024-05-01','end_date':'2024-12-31'}]})['provenance']
    write_json(directory / 'input/manifest.json', {'validation_plan': plan})
    result = import_results(store, identifier)
    assert validation_holdouts(Path(result['result_path'])) == plan['holdout_scenarios']


@pytest.mark.parametrize('owner_state', ['alive', 'dead', 'reused', 'unknown'])
def test_interrupted_preparation_recovery(job, monkeypatch, owner_state):
    """Only exited owners or reused PIDs unlock retry; live preparation stays intact."""
    import vast_jobs
    store, identifier, _ = job
    store.update(identifier, status='preparing', preparation_owner={'pid': 123, 'created_at': 10})

    class Process:
        """Isolated process identity without inspecting host processes."""
        def __init__(self, pid):
            if owner_state == 'dead':
                raise vast_jobs.psutil.NoSuchProcess(pid)
            if owner_state == 'unknown':
                raise vast_jobs.psutil.AccessDenied(pid)

        def create_time(self):
            """Simulate PID reuse."""
            return 11 if owner_state == 'reused' else 10

        def is_running(self):
            """Report the current mock process as running."""
            return True

    monkeypatch.setattr(vast_jobs.psutil, 'Process', Process)
    state = store.recover_interrupted_preparation(identifier)
    assert state['status'] == ('failed' if owner_state in {'dead', 'reused'} else 'preparing')


def test_external_deletion_of_active_rental_is_detected(job):
    """Two fresh spaced absences close an active rental without a cleanup request."""
    store, identifier, intent = job
    client = Provider([{'id': 123, 'label': intent['label'], 'actual_status': 'running'}])
    assert not guard_step(store, identifier, client, intent, '', now=1100)
    client.rows = []
    assert not guard_step(store, identifier, client, intent, '', now=1110)
    assert store.read(identifier)['rental_state'] == 'active'
    assert not guard_step(store, identifier, client, intent, '', now=1111)
    assert store.read(identifier)['absent_checks'] == 1
    assert guard_step(store, identifier, client, intent, '', now=1120)
    assert store.read(identifier)['status'] == 'failed'
    assert store.read(identifier)['rental_end_reason'] == 'provider_instance_missing'
    assert store.read(identifier)['rental_state'] == 'deletion_verified'
    run_loop(store, identifier)
    assert store.read(identifier)['status'] == 'failed'
    assert 'disappeared' in store.read(identifier)['error']
    assert client.calls == [('PUT', '/asks/42/')]
    assert guard_step(store, identifier, client, intent, '', now=1130)
    assert client.calls == [('PUT', '/asks/42/')]


@pytest.mark.parametrize('status', ['running', 'stopped', 'exited', 'offline'])
def test_reappearing_or_stopped_instance_resets_absence(job, status):
    """An existing instance, even stopped, is not a deleted rental."""
    store, identifier, intent = job
    client = Provider([{'id': 123, 'label': intent['label']}])
    guard_step(store, identifier, client, intent, '', now=1100)
    client.rows = []
    guard_step(store, identifier, client, intent, '', now=1110)
    client.rows = [{'id': 123, 'label': intent['label'], 'actual_status':status}]
    assert not guard_step(store, identifier, client, intent, '', now=1120)
    assert store.read(identifier)['absent_checks'] == 0
    assert store.read(identifier)['rental_state'] == 'active'
    assert len(client.calls) == 1


@pytest.mark.parametrize('status', [401, 403, 429, 503])
def test_provider_errors_require_two_new_absence_checks(job, status):
    """Provider/auth failures cannot complete or bridge deletion confirmation."""
    store, identifier, intent = job
    client = Provider([{'id':123, 'label':intent['label']}])
    guard_step(store, identifier, client, intent, '', now=1100)
    client.rows = []
    guard_step(store, identifier, client, intent, '', now=1110)

    class Unavailable(Provider):
        """An unavailable provider is never interpreted as an empty account."""
        def instances(self, *, fresh=False):
            """Require an uncached query and fail it explicitly."""
            assert fresh
            raise VastError('Provider unavailable', status)

    with pytest.raises(VastError):
        guard_step(store, identifier, Unavailable(), intent, '', now=1120)
    assert store.read(identifier)['absent_checks'] == 0
    assert store.read(identifier)['rental_state'] == 'active'
    assert not guard_step(store, identifier, client, intent, '', now=1130)
    assert guard_step(store, identifier, client, intent, '', now=1140)


def test_changed_label_cannot_confirm_external_deletion(job):
    """An instance still present by its ID must not release the rental lock."""
    store, identifier, intent = job
    client = Provider([{'id':123, 'label':intent['label']}])
    guard_step(store, identifier, client, intent, '', now=1100)
    client.rows = [{'id':123, 'label':'manually-renamed'}]
    with pytest.raises(VastError, match='ownership label changed'):
        guard_step(store, identifier, client, intent, '', now=1120)
    assert store.read(identifier)['rental_state'] == 'active'
    assert store.read(identifier)['absent_checks'] == 0
    assert len(client.calls) == 1


def test_confirmed_creation_gets_visibility_grace(job):
    """Do not close a newly allocated instance before its provider listing appears."""
    store, identifier, intent = job
    client = Provider()
    assert not guard_step(store, identifier, client, intent, '', now=1100)
    assert not guard_step(store, identifier, client, intent, '', now=1110)
    assert not store.read(identifier).get('absent_checks')
    assert not guard_step(store, identifier, client, intent, '', now=1220)
    assert guard_step(store, identifier, client, intent, '', now=1230)
    assert len(client.calls) == 1


def test_absence_confirmation_never_uses_cached_instance_list(job):
    """A stale cached list cannot supply repeated absence confirmations."""
    store, identifier, intent = job
    client = Provider([{'id':123, 'label':intent['label']}])
    guard_step(store, identifier, client, intent, '', now=1100)

    class FreshOnly(Provider):
        """The provider only permits forced fresh reads for confirmation."""
        def instances(self, *, fresh=False):
            """Expose absent data only after validating cache bypass."""
            assert fresh
            return []

    assert not guard_step(store, identifier, FreshOnly(), intent, '', now=1110)
    assert guard_step(store, identifier, FreshOnly(), intent, '', now=1120)


def test_ambiguous_creation_absence_recovers_without_manual_cleanup(job):
    """A timed-out creation can settle without another paid create or manual file removal."""
    store, identifier, intent = job
    client = Provider(fail=True)
    with pytest.raises(VastError):
        guard_step(store, identifier, client, intent, '', now=1100)
    client.fail = False
    assert not guard_step(store, identifier, client, intent, '', now=1110)
    assert not guard_step(store, identifier, client, intent, '', now=1220)
    assert guard_step(store, identifier, client, intent, '', now=1230)
    assert client.calls == [('PUT', '/asks/42/')]


@pytest.mark.parametrize('specific', [False, True])
def test_failed_optimizer_exit_is_not_result_import_failure(tmp_path, monkeypatch, specific):
    """An upstream execution failure needs no nonexistent-result import retry."""
    import vast_job_runner as runner
    store = JobStore(tmp_path / 'vast')
    state = store.create_preparation('failure', 20_000, 4, False)
    identifier = state['id']
    final = ensure_private_directory(store.directory(identifier) / 'final-results')
    write_json(final / 'finished.json', {'cancelled': False, 'exit_code': 1, 'wall_seconds': 3})
    (final / 'optimizer.log').write_text('suite mode does not support asymmetric live.approved_coins; token=secret' if specific else 'unexpected error')
    def forbidden(*args, **kwargs):
        """No valid results exist for the importer to process."""
        pytest.fail('Failed execution was incorrectly sent to result import')
    monkeypatch.setattr(runner, 'import_results', forbidden)
    row = runner.finalize_collected_results(store, identifier)
    assert row['status'] == 'failed' and row['exit_code'] == 1
    assert 'optimizer exited with code 1' in row['error']
    assert ('Suite requires identical' in row['error']) is specific
    assert 'secret' not in row['error'] and 'import needs retry' not in row['error']


def test_failed_optimizer_preserves_importable_partial_results(tmp_path, monkeypatch):
    """A nonzero exit does not discard collected evaluations that can still import."""
    import vast_job_runner as runner
    import vast_convergence
    store = JobStore(tmp_path / 'vast')
    identifier = store.create_preparation('partial_failure', 20_000, 4, False)['id']
    final = ensure_private_directory(store.directory(identifier) / 'final-results')
    write_json(final / 'finished.json', {'cancelled': False, 'exit_code': 1})
    results = ensure_private_directory(final / 'optimize_results' / 'run')
    (results / 'all_results.bin').write_bytes(b'collected')
    monkeypatch.setattr(runner, 'import_results', lambda *args: {'evaluations': 12})
    monkeypatch.setattr(vast_convergence, 'observe', lambda *args, **kwargs: None)
    row = runner.finalize_collected_results(store, identifier)
    assert row['exact_completed'] == 12
    assert row['status'] == 'failed' and 'exited with code 1' in row['error']


def test_gpu_constraint_safety_failure_explains_completion_boundary(tmp_path):
    """Surface the native safety cause without exposing arbitrary optimizer-log content."""
    import vast_job_runner as runner
    store = JobStore(tmp_path / 'vast')
    identifier = store.create_preparation('completion_failure', 20_000, 4, False)['id']
    final = ensure_private_directory(store.directory(identifier) / 'final-results')
    write_json(final / 'finished.json', {'cancelled': False, 'exit_code': 1})
    (final / 'optimizer.log').write_text(
        'backtest_completion_ratio_min: proxy=0.999994662 exact=1.0 mode=less_than bound=1.0\n'
        'GPU proxy/exact proxy-front constraint agreement fell below safety threshold\n'
        'token=secret')
    row = runner.finalize_collected_results(store, identifier)
    assert row['status'] == 'failed'
    assert 'Passivbot safety check' in row['error']
    assert 'strict completion limit 1.0' in row['error']
    assert 'secret' not in row['error']
