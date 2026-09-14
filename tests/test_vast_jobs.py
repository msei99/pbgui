"""Offline rental lifecycle and untrusted result transfer regression tests."""
import copy
import io
import json
import tarfile

import msgpack
import pytest

from secure_files import ensure_private_directory
from vast_jobs import JobStore, IMAGE, REVISION, native_job_config, write_json, digest
from vast_job_runner import guard_step, run_loop, validate_intent
from vast_provider import VastError
from vast_transfer import extract_results, import_results, fetch_host_key_result
from setup.vast_gpu_benchmark.cloud_worker import safe_path


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
    assert client.calls == [('DELETE', '/instances/123')]
    client.rows = [{'id': 999, 'label': 'foreign'}]
    assert not guard_step(store, identifier, client, intent, 'fake_token', now=2010)
    assert guard_step(store, identifier, client, intent, 'fake_token', now=2020)
    assert store.read(identifier)['rental_state'] == 'deletion_verified'
    assert store.read(identifier)['provider_status'] == 'deleted'


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
    monkeypatch.setattr(vast_provisioning_log, 'collect_provisioning_log', lambda *a: None)
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
              'bot': {'long':{}, 'short':{}}, 'backtest': {}, 'pbgui': {'sweep': 'keep'},
              'optimize': {'scoring':[{'metric':'gain_strategy_eq'}], 'limits':[]}}
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
              'bot': {'long':{}, 'short':{}}, 'backtest': {},
              'optimize': {'scoring':[{'metric':'adg_strategy_eq'}], 'limits':[]}}
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
