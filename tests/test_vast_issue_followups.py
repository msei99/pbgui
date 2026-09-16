"""Offline regressions for Vast issues 345, 350–353 and 361."""
import json
import shlex
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from api import vast
import vast_job_runner as runner
import vast_market_cache
from vast_jobs import JobStore, write_json
from vast_provider import VastError
from vast_queue import CloudQueue, worker_step
from vast_transfer import WorkerConnection


@pytest.fixture
def queue(tmp_path):
    """Provide one paid worker and one later-added job with temporary storage."""
    store = JobStore(tmp_path / 'vast')
    queue = CloudQueue(store)
    for identifier, state in [('a'*32, dict(kind='worker', status='idle', rental_state='active', workers=4,
            deadline=5000, deadline_protocol=2, transfer_reserved_used=.05, idle_since=1, idle_seconds=300)),
            ('b'*32, dict(status='ready', rental_state='none', workers=4, input_bytes=100))]:
        folder = store.root / 'jobs' / identifier
        folder.mkdir(parents=True)
        write_json(folder / 'state.json', dict(id=identifier, **state))
        write_json(folder / 'control.json', dict(stop=False, cleanup=False))
    write_json(store.directory('a'*32) / 'intent.json', dict(id='a'*32, accepted_at=0, deadline=5000,
        budget_usd=1, transfer_reserve_usd=.05, offer=dict(price_hour_usd=.5, download_gb_usd=0, upload_gb_usd=.02)))
    queue.update(worker_id='a'*32)
    return queue


def test_reserve_rebalance_waits_for_guard_acknowledgement(queue, monkeypatch):
    """A later job can use unused time budget only after a shorter deadline is confirmed."""
    monkeypatch.setattr('vast_deadline.time.time', lambda: 1000)
    assert worker_step(queue, 'a'*32, now=1000) is None
    worker = queue.store.read('a'*32)
    request = worker['deadline_request']
    assert request['transfer_reserve_usd'] == pytest.approx(.11)
    assert request['deadline'] < 5000
    assert not queue.store.read('a'*32, 'control.json')['cleanup']
    assert queue.store.read('b'*32)['status'] == 'ready'
    assert worker_step(queue, 'a'*32, now=1001) is None
    assert queue.store.read('a'*32)['deadline_request'] == request
    queue.store.update('a'*32, deadline_request=None, deadline_confirmed=request,
                       transfer_reserve_confirmed=request['transfer_reserve_usd'], deadline=request['deadline'])
    assert worker_step(queue, 'a'*32, now=1002) == 'b'*32
    assert queue.store.read('a'*32, 'intent.json')['budget_usd'] == 1


@pytest.mark.parametrize('protocol', [0, 1])
def test_old_guard_does_not_spend_unacknowledged_reserve(queue, protocol):
    """Unsupported guards retain a visible manual-recovery reason."""
    queue.store.update('a'*32, deadline_protocol=protocol, idle_since=None)
    assert worker_step(queue, 'a'*32, now=1000) is None
    assert 'Adjust Transfer reserve/Budget' in queue.store.read('b'*32)['error']
    assert not queue.store.read('a'*32).get('deadline_request')
    assert queue.store.read('b'*32)['status'] == 'ready'


def test_rejected_auto_reserve_is_not_resubmitted_forever(queue, monkeypatch):
    """An explicit worker rejection remains actionable instead of resetting idle forever."""
    monkeypatch.setattr('vast_deadline.time.time', lambda: 1000)
    worker_step(queue, 'a'*32, now=1000)
    queue.store.update('a'*32, deadline_request=None, deadline_error='Worker rejected the deadline change')
    worker_step(queue, 'a'*32, now=1001)
    assert queue.store.read('a'*32)['deadline_request'] is None
    assert 'cannot confirm' in queue.store.read('b'*32)['error']


def test_insufficient_budget_does_not_dispatch_later_job(queue, monkeypatch):
    """Reserve rebalancing cannot consume cleanup time or exceed authorized funds."""
    monkeypatch.setattr('vast_deadline.time.time', lambda: 4700)
    queue.store.update('a'*32, idle_since=None)
    assert worker_step(queue, 'a'*32, now=4700) is None
    assert '10 minutes' in queue.store.read('b'*32)['error']
    assert not queue.store.read('a'*32).get('deadline_request')


@pytest.mark.parametrize('payload_size', [65536, 65537, 131072])
def test_log_tail_has_transport_margin_and_bounded_local_copy(queue, tmp_path, monkeypatch, payload_size):
    """A bounded noisy transport does not fail at exactly 64 KiB of log output."""
    import vast_jobs
    monkeypatch.setattr(vast_jobs, 'PROJECT', tmp_path)
    calls = []

    def command(value, **kwargs):
        """Return bounded log bytes without any remote connection."""
        calls.append(kwargs)
        return b'x' * payload_size

    runner.sync_optimizer_log(SimpleNamespace(command=command, remote_root='/work/test'), queue.store, 'b'*32)
    assert calls == [{'max_output': 131072}]
    path = tmp_path / 'data/logs/optimizes_v8' / ('vast_' + 'b'*32 + '.log')
    assert path.stat().st_size == 65536
    assert queue.store.read('b'*32)['log_error'] is None


def test_log_failure_is_separate_from_job_failure(queue):
    """Telemetry oversize is recorded without aborting the caller's lifecycle step."""
    def command(*args, **kwargs):
        """Simulate a stream that exceeds even the safety margin."""
        raise VastError('SSH output exceeds the permitted transfer size')
    runner.sync_optimizer_log(SimpleNamespace(command=command, remote_root='/work/test'), queue.store, 'b'*32)
    assert 'transfer size' in queue.store.read('b'*32)['log_error']
    assert queue.store.read('b'*32)['status'] == 'ready'


def test_log_failure_cannot_prevent_final_collection(queue, monkeypatch):
    """Issue 353: even persistent telemetry failures leave result collection reachable."""
    identifier = 'a'*32
    queue.store.update(identifier, uploaded=True, worker_ready=True, started_at=100, status='running')
    monkeypatch.setattr(runner, 'validate_intent', lambda value, _: value)
    monkeypatch.setattr(runner, 'time', SimpleNamespace(time=lambda: 1000, sleep=lambda _: None))
    monkeypatch.setattr(runner, 'VastCredentialStore', lambda _: SimpleNamespace(secrets=lambda: {'api_key':'fake'}))
    monkeypatch.setattr(runner, 'VastClient', lambda _: object())
    monkeypatch.setattr(runner, 'owned_instance', lambda *a: {'id':7, 'actual_status':'running'})
    monkeypatch.setattr('vast_runtime_metrics.sample_metrics', lambda *a: None)

    class Collected(BaseException):
        """Exit after reaching final collection."""

    def command(*args, **kwargs):
        """Fail only the optional log stream."""
        raise VastError('SSH output exceeds the permitted transfer size')

    def collect(final, **kwargs):
        """Assert finished results still get fetched."""
        assert final
        raise Collected()

    remote = SimpleNamespace(bootstrap=lambda _: None, remote_root='/work/test', command=command,
        operation=lambda *a: {'started':True, 'finished':True, 'exact_completed':5}, collect=collect)
    monkeypatch.setattr(runner, 'WorkerConnection', lambda *a, **kw: remote)
    with pytest.raises(Collected):
        runner.run_loop(queue.store, identifier)
    assert queue.store.read(identifier)['status'] == 'collecting'


@pytest.mark.parametrize('minimum,cores,accepted', [(4, 4, True), (16, 4, False), (16, 16, True)])
def test_auto_cpu_uses_explicit_cloud_minimum(queue, monkeypatch, minimum, cores, accepted):
    """Local n_cpus does not silently become a cloud rental requirement."""
    queue.store.update('a'*32, rental_state='deletion_verified')
    queue.store.update('b'*32, auto_cpu_workers=True, workers=32)
    monkeypatch.setattr(vast, 'CloudQueue', lambda: queue)
    monkeypatch.setattr(vast, 'VastCredentialStore', lambda: SimpleNamespace(secrets=lambda: {'api_key': 'fake'}))
    offer = dict(id=42, gpu_name='RTX 5090', num_gpus=1, cuda_max_good=13, price_hour_usd=.2,
                 vram_gb=32, ram_gb=64, cpu_cores=cores, disk_gb=40, verified=True)
    queries = []

    def offers(**kwargs):
        """Return a provider row even when it violates the requested minimum."""
        queries.append(kwargs)
        return [offer]

    monkeypatch.setattr(vast, 'VastClient', lambda key: SimpleNamespace(offers=offers))
    monkeypatch.setattr(queue, 'start', lambda selected, *args: selected)
    body = vast.StartJobRequest(preferences=vast.GpuPreferences(min_cpu=minimum), accept_rental_and_cleanup=True)
    if accepted:
        assert vast.start_queue(body, session=None)['cpu_cores'] == cores
    else:
        with pytest.raises(HTTPException):
            vast.start_queue(body, session=None)
    assert queries[0]['min_cpu'] == minimum


@pytest.mark.parametrize('failure', [False, True])
def test_optimizer_starts_only_after_fresh_markets_are_staged(queue, tmp_path, monkeypatch, failure):
    """Legacy input without caches is made safe before the cloud optimizer starts."""
    identifier = 'b'*32
    directory = queue.store.directory(identifier)
    (directory / 'input').mkdir()
    write_json(directory / 'input/manifest.json', {'files': []})
    queue.store.update(identifier, exchanges=['bybit', 'binance'])
    remote = tmp_path / 'remote'
    launches = []
    stamp = time.time() - 10

    async def fetch(exchanges):
        """Fetch synthetic metadata on the PBGui side only."""
        if failure:
            raise OSError('local exchange unavailable')
        return {ex: {'markets': {'ETH/USDT:USDT': {'swap': True}}, 'fetched_at': stamp} for ex in exchanges}

    def command(value, stdin=None, **kwargs):
        """Execute only the metadata staging script locally; intercept worker start."""
        if 'nohup' in value:
            for ex in ('bybit', 'binance'):
                path = remote / 'output/caches' / ex / 'markets.json'
                assert path.is_file() and abs(path.stat().st_mtime - stamp) < .01
            launches.append(value)
        else:
            subprocess.run([sys.executable, '-c', shlex.split(value)[2]], stdin=stdin, check=True)
        return b''

    monkeypatch.setattr(vast_market_cache, 'fetch_markets', fetch)
    monkeypatch.setattr('vast_inception.stage_inception', lambda connection: None)
    connection = SimpleNamespace(store=queue.store, identifier=identifier, directory=directory,
                                 remote_root=str(remote), command=command)
    if failure:
        with pytest.raises(VastError, match='not started'):
            WorkerConnection.start(connection)
        assert not launches
    else:
        WorkerConnection.start(connection)
        assert len(launches) == 1


@pytest.mark.parametrize('snapshot', [{}, {'bybit': {'markets': {}, 'fetched_at': 1}},
                                     {'bybit': {'markets': {'x': {}}, 'fetched_at': 1}}])
def test_incomplete_or_expired_market_metadata_never_reaches_worker(monkeypatch, snapshot):
    """Freshness validation cannot silently turn partial snapshots into a remote fetch."""
    async def fetch(exchanges):
        """Return an invalid local snapshot."""
        return snapshot
    monkeypatch.setattr(vast_market_cache, 'fetch_markets', fetch)
    connection = SimpleNamespace(identifier='test', store=SimpleNamespace(read=lambda _: {'exchanges': ['bybit']}))
    with pytest.raises(VastError) as error:
        vast_market_cache.stage_public_markets(connection)
    assert error.value.status == 422
