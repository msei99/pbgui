"""Offline overlapping queue scans, failure recovery and immutable caller snapshots."""
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
import threading

import pytest

from api import backtest_v8 as bt


@pytest.mark.parametrize('fail', [False, True])
def test_overlapping_queue_reads_share_one_scan_and_release_registry(tmp_path, monkeypatch, fail):
    """Four simultaneous readers share one validated scan and independently receive its outcome."""
    entered, release, waiting = threading.Event(), threading.Event(), threading.Event()
    lock = threading.Lock()
    callers, scans = [], []

    class WaitingFuture(Future):
        """Expose waiter arrival without timing-dependent sleeps."""

        def result(self, *args, **kwargs):
            """Allow the test to release the producer after all three readers have joined."""
            with lock:
                callers.append(True)
                if len(callers) == 3:
                    waiting.set()
            return super().result(*args, **kwargs)

    def scan(_directory):
        """Hold one synthetic disk scan while concurrent readers arrive."""
        scans.append(True)
        entered.set()
        assert release.wait(3)
        if fail:
            raise ValueError('synthetic queue read failure')
        return [{'filename':'native', 'status':'complete', 'exchange':['bybit']}]

    monkeypatch.setattr(bt, '_queue_dir', lambda: tmp_path)
    monkeypatch.setattr(bt, '_scan_queue', scan)
    monkeypatch.setattr(bt, 'Future', WaitingFuture)
    monkeypatch.setattr(bt, '_log', lambda *_args, **_kwargs: None)
    with ThreadPoolExecutor(max_workers=4) as pool:
        requests = [pool.submit(bt._load_queue) for _ in range(4)]
        try:
            assert entered.wait(2)
            assert waiting.wait(2)
        finally:
            release.set()
        if fail:
            for request in requests:
                with pytest.raises(ValueError, match='synthetic queue read failure'):
                    request.result(timeout=3)
        else:
            results = [request.result(timeout=3) for request in requests]
            results[0][0]['exchange'].append('changed')
            assert all(result[0]['exchange'] == ['bybit'] for result in results[1:])
    assert len(scans) == 1
    assert not bt._queue_scans
    monkeypatch.setattr(bt, '_scan_queue', lambda _directory: [{'status':'queued'}])
    assert bt._load_queue() == [{'status':'queued'}]
    assert not bt._queue_scans


def test_concurrent_queue_roots_are_not_coalesced(tmp_path, monkeypatch):
    """A queue-root change must never borrow another root's state."""
    local = threading.local()
    entered = threading.Barrier(2)

    def scan(directory):
        """Both independent roots must be admitted concurrently."""
        entered.wait(timeout=3)
        return [{'root':directory.name}]

    def read(name):
        """Bind this reader to an independent temporary queue namespace."""
        local.root = tmp_path / name
        return bt._load_queue()

    monkeypatch.setattr(bt, '_queue_dir', lambda: local.root)
    monkeypatch.setattr(bt, '_scan_queue', scan)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(read, ['first', 'second']))
    assert results == [[{'root':'first'}], [{'root':'second'}]]
    assert not bt._queue_scans


def test_recursive_queue_read_does_not_wait_on_its_own_future(tmp_path, monkeypatch):
    """A reentrant reader preserves the preexisting direct-read behavior."""
    calls = []

    def scan(directory):
        """Reenter once to reproduce an indirect queue reader in a helper."""
        calls.append(directory)
        return bt._load_queue() if len(calls) == 1 else [{'status':'complete'}]

    monkeypatch.setattr(bt, '_queue_dir', lambda: Path(tmp_path))
    monkeypatch.setattr(bt, '_scan_queue', scan)
    assert bt._load_queue() == [{'status':'complete'}]
    assert len(calls) == 2 and not bt._queue_scans
