"""Offline timed upload recovery tests without SSH, rental changes or real sleeps."""
from types import SimpleNamespace

import pytest

import vast_job_runner as runner
from vast_jobs import JobStore, IMAGE, REVISION, digest, write_json
from vast_provider import VastError
from pb8_config import PB8RuntimeBusyError


class UploadFinished(BaseException):
    """Stop a simulated runner when it advances beyond input transfer."""


@pytest.fixture
def recovery(tmp_path, monkeypatch):
    """Provide a healthy worker with an old setup timer and a fake wall clock."""
    store = JobStore(tmp_path / 'vast')
    identifier = 'a' * 32
    folder = store.root / 'jobs' / identifier
    folder.mkdir(parents=True)
    (folder / 'input.tar.gz').write_bytes(b'test input')
    write_json(folder / 'state.json', dict(id=identifier, status='uploading', rental_state='active',
        worker_ready=True, setup_started_at=1, upload_attempts=2))
    write_json(folder / 'control.json', dict(stop=False, cleanup=False))
    write_json(folder / 'intent.json', dict(id=identifier, image=IMAGE, pb8_revision=REVISION,
        label='pbgui-vast-' + identifier, offer={'id': 7}, accepted_at=1, deadline=5000,
        budget_usd=1, bundle_sha256=digest(folder / 'input.tar.gz')))
    clock = [1000.0]
    sleeps = []

    def sleep(seconds):
        """Advance fake time while asserting cancellation is checked frequently."""
        assert 0 < seconds <= 5
        sleeps.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr(runner, 'time', SimpleNamespace(time=lambda: clock[0], sleep=sleep))
    monkeypatch.setattr(runner, 'VastCredentialStore', lambda _: SimpleNamespace(secrets=lambda: {'api_key': 'fake'}))
    monkeypatch.setattr(runner, 'VastClient', lambda _: object())
    monkeypatch.setattr(runner, 'owned_instance', lambda *a: {'id': 7, 'actual_status': 'running'})
    return store, identifier, clock, sleeps


def connection(monkeypatch, upload):
    """Stub transport and stop before running the optimizer."""
    def operation(*args, **kwargs):
        """Successful upload must lead to optimizer status, not another transfer."""
        raise UploadFinished()
    monkeypatch.setattr(runner, 'WorkerConnection', lambda *a, **kw: SimpleNamespace(
        bootstrap=lambda client: None, upload=upload, operation=operation))


def test_retries_for_minutes_then_succeeds_despite_old_setup_timer(recovery, monkeypatch):
    """Repeated early SSH timeouts no longer spend a two-attempt quota."""
    store, identifier, clock, sleeps = recovery
    calls = []

    def upload(timeout):
        """Fail ten times without sending data, then recover."""
        calls.append(clock[0])
        assert timeout > 180
        if len(calls) <= 10:
            raise VastError('SSH worker operation failed: connection timed out')

    connection(monkeypatch, upload)
    with pytest.raises(UploadFinished):
        runner.run_loop(store, identifier)
    assert len(calls) == 11
    assert calls[1] - calls[0] == 15
    assert calls[2] - calls[1] == 30
    assert calls[-1] - calls[0] >= 8 * 60
    state = store.read(identifier)
    assert state['uploaded'] and state['upload_attempts'] == 13
    assert state['upload_retry_at'] is None and state['error'] is None
    assert not store.read(identifier, 'control.json')['cleanup']


def test_no_progress_eventually_fails_after_full_recovery_window(recovery, monkeypatch):
    """A dead connection receives fifteen minutes but cannot retry forever."""
    store, identifier, clock, _ = recovery

    def upload(timeout):
        """Simulate immediate connection refusal on every attempt."""
        raise VastError('SSH worker operation failed: connection refused')

    connection(monkeypatch, upload)
    runner.run_loop(store, identifier)
    assert clock[0] >= 1900
    assert '15 minutes' in store.read(identifier)['error']
    assert 'connection refused' in store.read(identifier)['error']
    assert store.read(identifier)['status'] == 'failed'
    assert store.read(identifier, 'control.json')['cleanup']


def test_new_transfer_progress_renews_recovery_window(recovery):
    """Growing partial data earns recovery time; repeated same bytes do not."""
    store, identifier, clock, _ = recovery
    error = VastError('Rsync file synchronization failed: connection interrupted (exit 255); partial files retained')
    runner.schedule_upload_retry(store, identifier, error, 5000)
    clock[0] += 890
    store.update(identifier, upload_progress={'transferred_bytes': 1024, 'total': 4096})
    runner.schedule_upload_retry(store, identifier, error, 5000)
    assert store.read(identifier)['upload_recovery_since'] == clock[0]
    clock[0] += 890
    runner.schedule_upload_retry(store, identifier, error, 5000)
    clock[0] += 11
    with pytest.raises(VastError, match='15 minutes'):
        runner.schedule_upload_retry(store, identifier, error, 5000)


def test_local_pb8_update_lock_retries_without_discarding_upload(recovery):
    """A temporary local PB8 update lock cannot fail an otherwise resumable cloud upload."""
    store, identifier, clock, _ = recovery
    store.update(identifier, upload_progress={"transferred_bytes": 137_000_000, "total": 2_400_000_000})

    runner.schedule_upload_retry(
        store, identifier, PB8RuntimeBusyError("PB8 is being installed or updated"), 5000
    )

    state = store.read(identifier)
    assert state["status"] == "uploading"
    assert state["upload_retry_bytes"] == 137_000_000
    assert state["upload_retry_at"] == clock[0] + 15
    assert "retrying" in state["error"]


def test_retry_state_survives_controller_restart(recovery):
    """Reading persisted state cannot reset an already exhausted retry window."""
    store, identifier, clock, _ = recovery
    error = VastError('SSH worker operation failed: connection reset by peer')
    runner.schedule_upload_retry(store, identifier, error, 5000)
    clock[0] += 901
    with pytest.raises(VastError, match='15 minutes'):
        runner.schedule_upload_retry(JobStore(store.root), identifier, error, 5000)


@pytest.mark.parametrize('message,status', [('Invalid rsync destination', 422),
    ('SSH worker operation failed: host identity verification failed', 502),
    ('SSH worker operation failed: remote disk full', 502), ('Input checksum mismatch', 502)])
def test_permanent_failures_do_not_retry(recovery, monkeypatch, message, status):
    """Waiting must not bypass identity, disk or integrity failures."""
    store, identifier, clock, sleeps = recovery

    def upload(timeout):
        """Emit one permanent upload failure."""
        raise VastError(message, status)

    connection(monkeypatch, upload)
    runner.run_loop(store, identifier)
    assert not sleeps
    assert store.read(identifier)['error'] == message
    assert store.read(identifier)['status'] == 'failed'


def test_stop_during_backoff_is_responsive(recovery, monkeypatch):
    """Stop remains effective within five seconds while awaiting another attempt."""
    store, identifier, clock, _ = recovery

    def upload(timeout):
        """Record a transient error before requesting cancellation."""
        raise VastError('SSH operation interrupted; reconnecting')

    def sleep(seconds):
        """The user presses stop during the first retry wait."""
        clock[0] += seconds
        store.control(identifier, 'stop')

    connection(monkeypatch, upload)
    monkeypatch.setattr(runner.time, 'sleep', sleep)
    runner.run_loop(store, identifier)
    assert clock[0] <= 1005
    assert store.read(identifier)['status'] == 'cancelled'


def test_rental_deadline_limits_recovery(recovery, monkeypatch):
    """Backoff cannot extend the paid rental or spend its collection reserve."""
    store, identifier, clock, _ = recovery
    intent = store.read(identifier, 'intent.json')
    intent['deadline'] = 1200
    write_json(store.directory(identifier) / 'intent.json', intent)

    def upload(timeout):
        """Keep failing close to the original rental deadline."""
        raise VastError('SSH worker operation failed: connection refused')

    connection(monkeypatch, upload)
    runner.run_loop(store, identifier)
    assert clock[0] <= 1020
    assert 'collection reserve' in store.read(identifier)['error']
    assert store.read(identifier, 'intent.json')['deadline'] == 1200
