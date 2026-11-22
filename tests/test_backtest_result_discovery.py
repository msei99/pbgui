"""Offline result-reader lifecycle and filesystem boundary regressions."""

import json
from pathlib import Path
import subprocess
import sqlite3
import threading

import pytest

import backtest_result_discovery as discovery
import backtest_result_metadata as metadata
from concurrent.futures import Future


def test_isolated_reader_uses_current_files_without_importing_environment(tmp_path, monkeypatch):
    """An actual child reads fixtures, ignores hostile Python paths and sees new results."""
    root = tmp_path / 'results'
    root.mkdir()
    marker = tmp_path / 'executed'
    (tmp_path / 'sitecustomize.py').write_text(f"open({str(marker)!r}, 'w').write('bad')")
    monkeypatch.setenv('PYTHONPATH', str(tmp_path))
    worker = discovery.ResultDiscoveryWorker()
    try:
        assert worker.discover(root, root) == []
        path = root / 'new' / 'analysis.json'
        path.parent.mkdir()
        path.write_text('{}')
        assert worker.discover(root, root) == [path]
        assert not marker.exists()
        assert not worker._processes
    finally:
        worker.close()
    with pytest.raises(discovery.ResultDiscoveryUnavailable, match='shutting down'):
        worker.discover(root, root)


def test_scanner_rejects_external_and_relative_roots(tmp_path):
    """Explicit absolute containment is required independently of the API caller."""
    root, outside = tmp_path / 'root', tmp_path / 'outside'
    root.mkdir()
    outside.mkdir()
    for allowed, selected in ((root, outside), (Path('.'), root), (root, Path('.'))):
        with pytest.raises(ValueError):
            discovery.discover_result_paths(allowed, selected)


def test_identical_concurrent_reads_share_only_the_inflight_result(tmp_path, monkeypatch):
    """Followers share one current scan, get separate lists, and later loads rescan."""
    started, joined, release = threading.Event(), threading.Event(), threading.Event()
    created = []

    class ObservedFuture(Future):
        """Signal that the follower joined the producer before it can complete."""

        def result(self, *args, **kwargs):
            joined.set()
            return super().result(*args, **kwargs)

    class Process:
        """Hold one child until both callers are demonstrably in flight."""

        def __init__(self, *args, **kwargs):
            self.returncode = None
            self.stdin = self.stdout = self.stderr = None
            created.append(self)

        def communicate(self, input=None, **kwargs):
            started.set()
            assert release.wait(5)
            self.returncode = 0
            return json.dumps([str(tmp_path / 'analysis.json')]).encode(), b''

        def poll(self):
            return self.returncode

        def wait(self):
            return self.returncode

        def kill(self):
            self.returncode = -9
            release.set()

    monkeypatch.setattr(discovery, 'Future', ObservedFuture)
    monkeypatch.setattr(discovery.subprocess, 'Popen', Process)
    worker, outputs = discovery.ResultDiscoveryWorker(), []

    def read():
        """Capture this caller's independent collection."""
        outputs.append(worker.discover(tmp_path, tmp_path))

    producer, follower = threading.Thread(target=read), threading.Thread(target=read)
    producer.start()
    try:
        assert started.wait(5)
        follower.start()
        assert joined.wait(5)
    finally:
        release.set()
        producer.join(5)
        if follower.ident is not None:
            follower.join(5)
    try:
        assert len(created) == 1
        assert len(outputs) == 2 and outputs[0] == outputs[1]
        outputs[0].clear()
        assert outputs[1] == [tmp_path / 'analysis.json']
        assert worker.discover(tmp_path, tmp_path) == outputs[1]
        assert len(created) == 2 and not worker._reads
    finally:
        worker.close()


def test_child_start_failure_is_typed_and_does_not_consume_slots(tmp_path, monkeypatch):
    """An OS resource failure has a stable retryable reason and releases admission."""
    def fail(*args, **kwargs):
        """Simulate a resource limit without starting a child."""
        raise OSError('fixture process creation failure')

    monkeypatch.setattr(discovery.subprocess, 'Popen', fail)
    worker = discovery.ResultDiscoveryWorker()
    try:
        for _ in range(3):
            with pytest.raises(discovery.ResultDiscoveryUnavailable) as error:
                worker.discover(tmp_path, tmp_path)
            assert error.value.code == 'io'
        assert not worker._reads and not worker._processes
    finally:
        worker.close()


def test_isolated_cache_page_reuses_fingerprints_and_rejects_stale_rows(tmp_path, monkeypatch):
    """A real child reads only the fixture cache and never accepts changed source files."""
    reader = tmp_path / 'backtest_result_discovery.py'
    reader.write_text(Path(discovery.__file__).read_text())
    (tmp_path / 'backtest_result_metadata.py').write_text(Path(metadata.__file__).read_text())
    monkeypatch.setattr(discovery, '__file__', str(reader))
    root = tmp_path / 'results'
    result = root / 'config' / 'run'
    result.mkdir(parents=True)
    analysis = result / 'analysis.json'
    analysis.write_text('{"gain": 2}')
    cache = tmp_path / 'data' / 'cache' / 'backtest_results.sqlite'
    cache.parent.mkdir(parents=True)
    signature = metadata.result_signature(analysis, version='v8', gui_version='fixture', summary_schema=4)
    with sqlite3.connect(cache) as connection:
        connection.execute('CREATE TABLE result_summaries(version, result_path, signature, payload)')
        connection.execute('INSERT INTO result_summaries VALUES (?, ?, ?, ?)',
            ('v8', str(result), signature, json.dumps({'path': str(result), 'gain': 2})))
    worker = discovery.ResultDiscoveryWorker()
    try:
        assert worker.cached_page(root, [analysis], 'fixture', 4) == {
            str(result): {'path': str(result), 'gain': 2}}
        alias = tmp_path / 'result-root-alias'
        alias.symlink_to(root, target_is_directory=True)
        discovered = worker.discover(alias, alias)
        assert discovered == [analysis]
        assert worker.cached_page(alias, discovered, 'fixture', 4) == {
            str(result): {'path': str(result), 'gain': 2}}
        assert worker.cached_page(root, [analysis], 'new-version', 4) == {}
        analysis.write_text('{"gain": 3, "new": true}')
        assert worker.cached_page(root, [analysis], 'fixture', 4) == {}
        with sqlite3.connect(cache) as connection:
            assert json.loads(connection.execute('SELECT payload FROM result_summaries').fetchone()[0])['gain'] == 2
    finally:
        worker.close()


def test_reader_timeout_kills_reaps_and_releases_admission(tmp_path, monkeypatch):
    """Timed-out child processes cannot survive their request or exhaust later slots."""
    processes = []

    class Process:
        """Stand in for a blocked child with the subprocess communication contract."""

        def __init__(self, argv, **kwargs):
            assert argv[1:3] == ['-I', '-S']
            assert set(kwargs['env']) <= {'LANG', 'PATH', 'SystemRoot'}
            self.returncode = None
            self.stdin = self.stdout = self.stderr = None
            self.killed = self.waited = False
            processes.append(self)

        def communicate(self, input=None, timeout=None):
            """Simulate a timeout, then allow the mandatory drain after kill."""
            if timeout is not None:
                raise subprocess.TimeoutExpired('fixture', timeout)
            return b'', b''

        def kill(self):
            """Mark only this owned child terminated."""
            self.killed, self.returncode = True, -9

        def poll(self):
            """Expose the child's current exit state."""
            return self.returncode

        def wait(self):
            """Record mandatory reaping."""
            self.waited = True
            return self.returncode

    monkeypatch.setattr(discovery.subprocess, 'Popen', Process)
    worker = discovery.ResultDiscoveryWorker()
    try:
        for _ in range(3):
            with pytest.raises(discovery.ResultDiscoveryUnavailable, match='timed out'):
                worker.discover(tmp_path, tmp_path)
        assert all(process.killed and process.waited for process in processes)
        assert not worker._processes
    finally:
        worker.close()


def test_close_terminates_owned_reads_and_bounds_concurrent_admission(tmp_path, monkeypatch):
    """Shutdown interrupts readers; a third parallel caller cannot queue unbounded work."""
    started = threading.Barrier(3)

    class Process:
        """A child stays blocked until the owning worker closes."""

        def __init__(self, *args, **kwargs):
            self.returncode = None
            self.stdin = self.stdout = self.stderr = None
            self.stopped = threading.Event()

        def communicate(self, input=None, **kwargs):
            """Keep the reader live until controlled shutdown."""
            started.wait(timeout=5)
            assert self.stopped.wait(5)
            return json.dumps([]).encode(), b''

        def kill(self):
            """Unblock both owned readers."""
            self.returncode = -9
            self.stopped.set()

        def poll(self):
            """Report controlled termination."""
            return self.returncode

        def wait(self):
            """Reap without creating another process."""
            return self.returncode

    monkeypatch.setattr(discovery.subprocess, 'Popen', Process)
    worker = discovery.ResultDiscoveryWorker()
    errors = []

    def read(index):
        """Record a shutdown rejection without leaking a thread exception."""
        try:
            worker.discover(tmp_path, tmp_path / str(index))
        except discovery.ResultDiscoveryUnavailable as exc:
            errors.append(str(exc))

    threads = [threading.Thread(target=read, args=(index,)) for index in range(2)]
    for thread in threads:
        thread.start()
    try:
        started.wait(timeout=5)
        with pytest.raises(discovery.ResultDiscoveryUnavailable, match='busy'):
            worker.discover(tmp_path, tmp_path / 'third')
    finally:
        worker.close()
        for thread in threads:
            thread.join(timeout=5)
    assert all(not thread.is_alive() for thread in threads)
    assert len(errors) == 2
    assert not worker._processes
