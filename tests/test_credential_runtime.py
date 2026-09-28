"""Isolated credential rotation, crash recovery and exact-process adoption tests."""
from __future__ import annotations

import json
from pathlib import Path
import signal
from types import SimpleNamespace

import pytest
import psutil

import PBRun as pbrun
import credential_runtime as runtime
from credential_runtime import CredentialRuntimeJournal
from pb7_api_keys import PB7ApiKeysMergeWriter


@pytest.fixture
def setup(tmp_path, monkeypatch):
    """Create an isolated runtime and deterministic clock without real credentials."""
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(runtime.time, 'time', lambda: clock.now)
    monkeypatch.setattr(pbrun, 'time', lambda: clock.now)
    monkeypatch.setattr(pbrun, '_log', lambda *args, **kwargs: None)
    directory = tmp_path / 'pb7'
    directory.mkdir()
    writer = PB7ApiKeysMergeWriter(directory / 'api-keys.json', tmp_path / 'data/credentials/pb7_projection.json')
    writer.write_exchange_payload({'alice': {'exchange': 'hyperliquid', 'private_key': 'old-secret'},
                                   'bob': {'exchange': 'bybit', 'secret': 'unchanged-secret'}})
    journal = CredentialRuntimeJournal(writer.api_keys_path, writer.projection_status_path)
    return SimpleNamespace(root=tmp_path, writer=writer, journal=journal, clock=clock)


def _rotate(setup, secret='new-secret'):
    """Replace only Alice's credential through the production writer."""
    payload = setup.writer.read()
    payload['alice']['private_key'] = secret
    setup.clock.now += 10
    setup.writer.write_exchange_payload(payload)


class Process:
    """A process identity with configurable graceful signal behaviour."""
    def __init__(self, runner, pid, created):
        self.runner, self.pid, self.created = runner, pid, created
        self.signals = []
        self.timeouts = 0

    def create_time(self):
        """Expose an immutable start timestamp for PID-reuse protection."""
        return self.created

    def is_running(self):
        """Return whether this exact fake process remains alive."""
        return self.runner.current is self

    def send_signal(self, sig):
        """Record the signal without touching any real process."""
        self.signals.append(sig)

    def wait(self, timeout):
        """Simulate either timeout escalation or a completed graceful exit."""
        if self.timeouts:
            self.timeouts -= 1
            raise psutil.TimeoutExpired(timeout)
        self.runner.current = None
        return 0


def _runner(setup, name='instance-a', user='alice'):
    """Bind the real reconciliation and launch decorator to a fake process runner."""
    folder = setup.root / 'data/run_v7' / name
    folder.mkdir(parents=True, exist_ok=True)
    runner = SimpleNamespace(user=name, path=str(folder), pbgdir=setup.root, pb7dir=setup.root/'pb7',
                             _v7_config={'live': {'user': user}}, starts=0, allowed=True, fail=False)
    runner.current = Process(runner, 10, 105)
    runner.pid = lambda: runner.current

    @pbrun._credential_launch('pb7')
    def start(self):
        """Apply a desired-state gate before creating the replacement process."""
        if not self.allowed:
            return False
        if self.fail:
            raise OSError('fake start failure')
        self.starts += 1
        self.current = Process(self, 10 + self.starts, setup.clock.now + 0.1)
        return True

    runner.start = start.__get__(runner)
    return runner


def test_only_changed_accounts_rotate_and_metadata_does_not(setup):
    """Serials, expiry metadata and another user must not cause unrelated restarts."""
    initial = setup.journal.recover(setup.writer.read())['users']
    payload = setup.writer.read()
    payload['_api_serial'] = 99
    payload['alice']['hl_valid_until'] = 123456
    payload['tradfi'] = {'api_key': 'unrelated'}
    setup.writer.write_exchange_payload(payload)
    assert setup.journal.recover(setup.writer.read())['users'] == initial
    _rotate(setup)
    after = setup.journal.recover(setup.writer.read())['users']
    assert after['alice']['revision'] != initial['alice']['revision']
    assert after['bob']['revision'] == initial['bob']['revision']
    setup.writer.write_exchange_payload(setup.writer.read())
    assert setup.journal.recover(setup.writer.read())['users'] == after


@pytest.mark.parametrize('after_replace', [False, True])
def test_crash_at_key_replacement_is_recovered(setup, monkeypatch, after_replace):
    """A failed write never releases new intent; a completed replacement is recoverable."""
    old = setup.journal.recover(setup.writer.read())['users']['alice']['revision']
    write = setup.writer._write_api_keys_unlocked

    def fail(payload):
        """Inject a crash before or after the atomic key replacement."""
        if after_replace:
            write(payload)
        raise OSError('simulated crash')

    monkeypatch.setattr(setup.writer, '_write_api_keys_unlocked', fail)
    with pytest.raises(OSError):
        _rotate(setup)
    assert setup.journal.public_status()['status'] == 'projecting'
    recovered = setup.journal.recover(setup.writer.read())
    assert (recovered['users']['alice']['revision'] != old) is after_replace
    assert 'pending' not in recovered


def test_rotation_restarts_only_affected_process_and_survives_pbrun_restart(setup):
    """Once adopted, duplicate delivery and new runner objects do not restart again."""
    alice, bob = _runner(setup), _runner(setup, 'bob-bot', 'bob')
    old = alice.current
    _rotate(setup)
    assert pbrun._reconcile_credentials(alice, 'pb7') is True
    assert old.signals == [signal.SIGINT]
    assert alice.starts == 1
    assert pbrun._reconcile_credentials(bob, 'pb7') is False
    assert bob.starts == 0
    fresh_runner = _runner(setup)
    fresh_runner.current = alice.current
    assert pbrun._reconcile_credentials(fresh_runner, 'pb7') is False
    assert fresh_runner.starts == 0
    status = setup.journal.public_status()
    assert status['items'][1]['status'] == 'applied'
    assert 'old-secret' not in setup.journal.path.read_text()
    assert 'new-secret' not in setup.journal.path.read_text()
    assert 'fingerprint' not in json.dumps(status)
    assert 'salt' not in json.dumps(status)


def test_multiple_instances_using_one_account_each_adopt(setup):
    """Account reuse must not let the first instance acknowledge all other instances."""
    first, second = _runner(setup), _runner(setup, 'instance-b')
    _rotate(setup)
    pbrun._reconcile_credentials(first, 'pb7')
    pbrun._reconcile_credentials(second, 'pb7')
    assert first.starts == second.starts == 1


def test_manual_restart_after_arrival_is_not_repeated(setup):
    """A manually replaced process newer than verified arrival is already current."""
    _rotate(setup)
    runner = _runner(setup)
    runner.current.created = setup.clock.now + 1
    assert pbrun._reconcile_credentials(runner, 'pb7') is False
    assert runner.starts == 0
    assert setup.journal.public_status()['items'][0]['status'] == 'applied'


def test_no_acknowledgement_of_newer_rotation_during_launch(setup):
    """A delayed start acknowledgement may never claim a later key revision."""
    runner = _runner(setup)
    runner.current = None

    @pbrun._credential_launch('pb7')
    def delayed(self):
        """Rotate again after capturing launch intent but before returning."""
        self.current = Process(self, 20, setup.clock.now + 0.1)
        _rotate(setup)
        return True

    delayed(runner)
    assert setup.journal.public_status()['items'][0]['status'] == 'pending'
    assert pbrun._reconcile_credentials(runner, 'pb7') is True
    assert runner.starts == 1


def test_failed_start_retries_without_acknowledging(setup):
    """Failed replacement stays visible and retries only after the persisted delay."""
    runner = _runner(setup)
    runner.fail = True
    _rotate(setup)
    pbrun._reconcile_credentials(runner, 'pb7')
    assert runner.current is None
    assert setup.journal.public_status()['items'][0]['status'] == 'error'
    runner.fail = False
    assert runner.start() is False
    setup.clock.now += 31
    runner.start()
    assert setup.journal.public_status()['items'][0]['status'] == 'applied'


def test_stop_failure_is_bounded_and_never_launches_duplicate(setup):
    """A process refusing every signal remains an error without a duplicate start."""
    runner = _runner(setup)
    old = runner.current
    old.timeouts = 10
    _rotate(setup)
    pbrun._reconcile_credentials(runner, 'pb7')
    assert old.signals == [signal.SIGINT, signal.SIGTERM, signal.SIGKILL]
    assert runner.starts == 0
    assert setup.journal.public_status()['items'][0]['status'] == 'error'
    pbrun._reconcile_credentials(runner, 'pb7')
    assert len(old.signals) == 3


def test_process_identity_change_aborts_stop(setup):
    """PID reuse or an external replacement must not be killed by an old request."""
    runner = _runner(setup)
    old = runner.current
    runner.current = Process(runner, old.pid, old.created + 1)
    assert pbrun._stop_for_credentials(runner, old) is False
    assert runner.current.signals == []


def test_desired_stop_wins_over_pending_rotation(setup):
    """A concurrent stop/reassignment may prevent the replacement start."""
    runner = _runner(setup)
    runner.allowed = False
    _rotate(setup)
    pbrun._reconcile_credentials(runner, 'pb7')
    assert runner.current is None
    assert runner.starts == 0
    assert setup.journal.public_status()['items'][0]['status'] == 'waiting'


def test_missing_credentials_do_not_stop_existing_bot(setup):
    """A deleted account cannot be treated as a successful credential rotation."""
    runner = _runner(setup)
    payload = setup.writer.read()
    del payload['alice']
    setup.writer.write_exchange_payload(payload)
    pbrun._reconcile_credentials(runner, 'pb7')
    assert runner.current.signals == []
    assert runner.starts == 0
    assert setup.journal.public_status()['items'][0]['status'] == 'error'


def test_corrupt_or_symlinked_journal_fails_closed(setup):
    """Unreadable adoption state must not stop a live bot or overwrite the journal."""
    runner = _runner(setup)
    setup.journal.path.write_text('{broken')
    assert pbrun._reconcile_credentials(runner, 'pb7') is True
    assert runner.current.signals == []
    assert setup.journal.path.read_text() == '{broken'
    setup.journal.path.unlink()
    outside = setup.root / 'outside'
    outside.write_text('sentinel')
    setup.journal.path.symlink_to(outside)
    assert pbrun._reconcile_credentials(runner, 'pb7') is True
    assert outside.read_text() == 'sentinel'


def test_untracked_baseline_does_not_restart_unchanged_accounts(tmp_path, monkeypatch):
    """Rolling out the journal must not restart every bot for an unrelated key edit."""
    directory = tmp_path/'pb7'
    directory.mkdir()
    path = directory/'api-keys.json'
    path.write_text(json.dumps({'alice': {'key': 'old'}, 'bob': {'key': 'same'}}))
    writer = PB7ApiKeysMergeWriter(path, tmp_path/'data/credentials/pb7_projection.json')
    writer.write_exchange_payload({'alice': {'key': 'new'}, 'bob': {'key': 'same'}})
    journal = CredentialRuntimeJournal(path, writer.projection_status_path)
    users = journal.recover(writer.read())['users']
    assert users['bob']['ready_at'] == 0
    assert users['alice']['ready_at'] > 0
    assert journal.path.stat().st_mode & 0o777 == 0o600


def test_public_status_is_read_only_and_stale_is_not_success(setup):
    """Status reads neither recover an intent nor claim indefinitely fresh adoption."""
    runner = _runner(setup)
    pbrun._reconcile_credentials(runner, 'pb7')
    before = setup.journal.path.read_bytes()
    setup.clock.now += 91
    assert setup.journal.public_status()['items'][0]['status'] == 'stale'
    assert setup.journal.path.read_bytes() == before


@pytest.mark.parametrize('version', ['pb7', 'pb8'])
def test_real_watchers_dispatch_rotation_and_respect_cluster_stop(setup, monkeypatch, version):
    """Both production watch methods reconcile rotated credentials after their run gate."""
    runner = _runner(setup)
    if version == 'pb8':
        pb8 = setup.root / 'pb8'
        pb8.mkdir()
        writer = PB7ApiKeysMergeWriter(pb8/'api-keys.json', setup.root/'data/credentials/pb8_projection.json')
        writer.write_exchange_payload(setup.writer.read())
        setup.writer = writer
        setup.journal = CredentialRuntimeJournal(writer.api_keys_path, writer.projection_status_path)
        runner.pb8dir = pb8
        runner.live_user = 'alice'
        original = runner.start.__wrapped__
        runner.start = pbrun._credential_launch('pb8')(original).__get__(runner)
    _rotate(setup)
    runner.cluster_blocked = False
    runner.cluster_gate = ''
    runner._cluster_gate_allows_run = lambda: True
    runner._cluster_gate_result = lambda: {'ok': True, 'status': 'ready'}
    runner.is_running = lambda: runner.current is not None
    runner.load = lambda: True
    watch = pbrun.RunV7.watch if version == 'pb7' else pbrun.RunV8.watch
    watch(runner)
    assert runner.starts == 1
    assert setup.journal.public_status()['items'][0]['status'] == 'applied'
    _rotate(setup, 'third-secret')
    runner._cluster_gate_allows_run = lambda: False
    runner._cluster_gate_result = lambda: {'ok': False, 'status': 'desired_stopped', 'reason': 'Stopped'}
    runner._log_block = lambda *args: None
    runner.stop = lambda *args: setattr(runner, 'current', None)
    watch(runner)
    assert runner.current is None
    assert runner.starts == 1


@pytest.mark.parametrize('field,value', [('revision', ''), ('fingerprint', {}), ('present', 'yes'), ('ready_at', 'corrupt'), ('ready_at', float('inf'))])
def test_corrupt_account_revision_never_acknowledges_or_restarts(setup, field, value):
    """Malformed persisted fields must not silently become an adopted baseline."""
    runner = _runner(setup)
    payload = json.loads(setup.journal.path.read_text())
    payload['users']['alice'][field] = value
    raw = json.dumps(payload)
    setup.journal.path.write_text(raw)
    assert pbrun._reconcile_credentials(runner, 'pb7') is True
    assert runner.starts == 0
    assert runner.current.signals == []
    assert setup.journal.path.read_text() == raw


def test_equivalent_runtime_path_uses_existing_journal(setup):
    """Lexical parent components must not make a valid configured path look foreign."""
    _rotate(setup)
    runner = _runner(setup)
    runner.pb7dir = setup.root/'ignored'/ '..'/'pb7'
    assert pbrun._reconcile_credentials(runner, 'pb7') is True
    assert runner.starts == 1


def test_changed_runtime_path_does_not_reuse_old_acknowledgements(setup):
    """A legitimate runtime path change can establish a new independent baseline."""
    new_root = setup.root/'replacement-pb7'
    new_root.mkdir()
    new_path = new_root/'api-keys.json'
    payload = setup.writer.read()
    new_path.write_text(json.dumps(payload))
    journal = CredentialRuntimeJournal(new_path, setup.writer.projection_status_path)
    assert journal.public_status()['status'] == 'not_tracked'
    runner = _runner(setup)
    runner.pb7dir = new_root
    assert pbrun._credential_context(runner, 'pb7') is None
    PB7ApiKeysMergeWriter(new_path, setup.writer.projection_status_path).write_exchange_payload(payload)
    assert journal.recover(payload)['users']['alice']['ready_at'] == 0
    assert journal.public_status()['items'] == []
