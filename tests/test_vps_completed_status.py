"""Current-run terminal results must clear stale overview/deployment state."""
from pathlib import Path
from types import SimpleNamespace

import pytest

import vps_manager_core as core
import vps_manager_service as service


@pytest.mark.parametrize('status', ['successful', 'failed', 'canceled', 'timeout'])
def test_finished_uses_actual_runner_result(status, monkeypatch, tmp_path):
    """Ansible's finished callback persists terminal status even after stale reloads."""
    monkeypatch.setattr(core, 'PBGDIR', tmp_path)
    vps = core.VPS()
    vps.hostname = 'example-host'
    vps.command_run_id = 'run-current'
    vps.update_status = 'starting'
    vps.save()
    monkeypatch.setattr(core, '_cleanup_runner_private_data_dir', lambda path: None)
    vps.update_finished(SimpleNamespace(status=status), private_data_dir='/tmp/owned', run_id='run-current')
    assert vps.update_status == status


def test_previous_run_callbacks_cannot_complete_new_run(monkeypatch):
    """Late callbacks clean only their owned private directory, not newer state."""
    vps = core.VPS()
    vps.command_run_id = 'run-new'
    vps.update_status = 'starting'
    vps.save = lambda: pytest.fail('stale callback persisted a newer run')
    cleaned = []
    monkeypatch.setattr(core, '_cleanup_runner_private_data_dir', cleaned.append)
    vps._append_task_log = lambda *a, **kw: pytest.fail('stale event wrote into a newer log')
    vps.update_event_handler({'stdout': 'PLAY RECAP'}, run_id='run-old')
    vps.update_status_handler({'status': 'successful'}, None, run_id='run-old')
    vps.update_finished(SimpleNamespace(status='successful'), private_data_dir='/tmp/old', run_id='run-old')
    assert vps.update_status == 'starting'
    assert cleaned == ['/tmp/old']


def test_missing_current_log_does_not_fall_back_to_old_recap(tmp_path):
    """The previous alias/buffer must not complete a new run that has no log yet."""
    alias = tmp_path / 'old.log'
    alias.write_text('PLAY RECAP\nexample-host : ok=1 failed=0 unreachable=0')
    vps = SimpleNamespace(command='vps-update-pb', command_run_id='run-new',
                          update_log=alias.read_text(), _task_log_path=lambda *a: tmp_path / 'new.log',
                          _task_log_alias_path=lambda *a: alias)
    assert service._read_vps_update_log_tail(vps) == ''


@pytest.mark.parametrize('recap,expected', [({'failed': 0, 'unreachable': 0}, 'successful'),
                                          ({'failed': 1, 'unreachable': 0}, 'failed'),
                                          (None, 'starting')])
def test_refresh_reconciles_only_completed_current_run(tmp_path, monkeypatch, recap, expected):
    """Automatic reads recover matching terminal logs without interrupting active runs."""
    instance = object.__new__(service.VPSManagerService)
    monkeypatch.setattr(core, 'PBGDIR', tmp_path)
    vps = core.VPS()
    vps.hostname = 'example-host'
    vps.command = 'vps-update-pb'
    vps.command_run_id = 'run-1'
    vps.update_status = 'starting'
    vps.save()
    monkeypatch.setattr(service, '_read_vps_update_log_tail', lambda target: 'current log')
    monkeypatch.setattr(service, '_parse_ansible_task_progress', lambda *a, **kw: {'recap': recap})
    monkeypatch.setattr(service, '_log', lambda *a, **kw: None)
    instance._recover_completed_vps_run(vps)
    assert vps.update_status == expected


def test_old_object_cannot_complete_a_newer_persisted_run(tmp_path, monkeypatch):
    """A second process may have launched a new run before reconciliation commits."""
    monkeypatch.setattr(core, 'PBGDIR', tmp_path)
    old = core.VPS()
    old.hostname = 'example-host'
    old.command_run_id = 'run-old'
    old.update_status = 'starting'
    old.save()
    newer = core.VPS()
    newer.load(old.path / 'example-host.json')
    newer.command_run_id = 'run-new'
    newer.save()
    assert old._persist_update_result('successful', 'run-old', only_running=True) is False
    newer.load(old.path / 'example-host.json')
    assert newer.command_run_id == 'run-new' and newer.update_status == 'starting'
