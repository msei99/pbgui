"""Imported runtime playbooks expose actual, ordered intermediate progress."""
from pathlib import Path
import threading

import pytest
import yaml

import vps_manager_service as service


@pytest.fixture
def playbooks(tmp_path, monkeypatch):
    """Use only small local YAML fixtures and a fresh source-inventory cache."""
    monkeypatch.setattr(service, 'PBGDIR', tmp_path)
    monkeypatch.setattr(service, '_PLAYBOOK_TASK_CACHE', {})

    def write(name, value):
        """Write a project-like playbook below the isolated approved root."""
        path = tmp_path/name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(value, sort_keys=False))

    write('bulk.yml', [{'import_playbook': 'runtime.yml', 'vars': {'runtime_product': 'pb8'}},
                       {'import_playbook': 'runtime.yml', 'vars': {'runtime_product': 'pb9'}}])
    write('runtime.yml', [{'hosts': 'fixture', 'gather_facts': False, 'tasks': [
        {'name': 'Validate {{ passivbot_label }}'},
        {'name': 'Prepare {{ passivbot_label }}', 'include_tasks': 'tasks/wheel.yml'},
        {'name': 'Common step'},
        {'name': 'Finish {{ passivbot_label }}'},
    ], 'handlers': [{'name': 'Restart service'}]}])
    write('tasks/wheel.yml', [
        {'name': "Build {{ passivbot_label | default('PB8') }} wheel"},
        {'block': [{'name': 'Verify {{ passivbot_major }} wheel'}]},
    ])
    return tmp_path, write


def test_imported_runtime_names_and_nested_tasks_are_rendered_without_execution(playbooks):
    """Both generations and their imported wheel tasks remain independently counted."""
    tasks = service._playbook_task_names('bulk')
    assert tasks == (
        'Validate PB8', 'Prepare PB8', 'Build PB8 wheel', 'Verify 8 wheel',
        'Common step', 'Finish PB8', 'Restart service',
        'Validate PB9', 'Prepare PB9', 'Build PB9 wheel', 'Verify 9 wheel',
        'Common step', 'Finish PB9', 'Restart service',
    )
    assert tasks.count('Common step') == 2


def test_progress_advances_across_imports_duplicates_and_early_handlers(playbooks):
    """Use real task headers, never a timer or the first duplicate's static index."""
    transcript = ''
    checkpoints = []
    instance = object.__new__(service.VPSManagerService)
    for line in ['TASK [Validate PB8]', 'TASK [Build PB8 wheel]',
                 'RUNNING HANDLER [Restart service]', 'TASK [Common step]',
                 'TASK [Finish PB8]', 'TASK [Validate PB9]',
                 'TASK [Build PB9 wheel]', 'TASK [Common step]', 'TASK [Finish PB9]']:
        transcript += line+'\n'
        parsed = service._parse_ansible_task_progress('fixture', transcript, command='bulk')
        checkpoints.append(instance._progress_payload('running', parsed['current_index'], parsed['total_steps'])['percent'])
    assert checkpoints == sorted(checkpoints)
    assert checkpoints[1] > 0
    assert checkpoints[2] == checkpoints[1]  # An early flush does not skip remaining tasks.
    assert checkpoints[-1] > checkpoints[4]
    assert checkpoints[-1] < 100
    parsed = service._parse_ansible_task_progress('fixture', transcript+
        'PLAY RECAP\nfixture : ok=14 changed=3 unreachable=0 failed=0 skipped=0 rescued=0 ignored=0\n', command='bulk')
    assert parsed['recap']['failed'] == 0
    assert instance._progress_payload('successful', parsed['current_index'], parsed['total_steps'])['percent'] == 100


def test_unknown_current_label_keeps_last_observed_progress(playbooks):
    """A dynamic or newly added task cannot reset established progress to zero."""
    parsed = service._parse_ansible_task_progress('fixture',
        'TASK [Validate PB9]\nTASK [A dynamic task without a static label]\n', command='bulk')
    assert parsed['current_index'] == 7
    assert parsed['step'] == 'A dynamic task without a static label'


def test_inventory_rejects_external_files_cycles_and_lookup_execution(playbooks, tmp_path):
    """Static discovery never evaluates Jinja, traverses outward or follows cycles."""
    root, write = playbooks
    outside = tmp_path.parent/(tmp_path.name+'-outside.yml')
    outside.write_text('- tasks:\n  - name: Outside task\n')
    (root/'link.yml').symlink_to(outside)
    write('unsafe.yml', [
        {'import_playbook': 'unsafe.yml'}, {'import_playbook': 'link.yml'},
        {'import_playbook': str(outside)},
        {'tasks': [{'name': 'Visible', 'include_tasks': "{{ lookup('pipe', 'touch marker') }}"}]},
    ])
    assert service._playbook_task_names('unsafe') == ('Visible',)
    assert service._playbook_task_names('../'+outside.stem) == ()
    assert not (root/'marker').exists()


@pytest.mark.parametrize('command', ['vps-update-runtime', 'vps-update-pbgui-runtime', 'vps-update-pb9'])
def test_real_composed_catalog_contains_runtime_and_wheel_tasks(command, monkeypatch):
    """Current composition wrappers cannot quietly become empty task inventories."""
    monkeypatch.setattr(service, '_PLAYBOOK_TASK_CACHE', {})
    tasks = service._playbook_task_names(command)
    assert len(tasks) > 50
    assert any('PB9' in name and 'wheel' in name.lower() for name in tasks)
    assert not any('{{' in name for name in tasks)


def test_running_progress_reserves_completion_for_terminal_outcome():
    """A complete static plan is not proof that the Ansible runner has finished."""
    instance = object.__new__(service.VPSManagerService)
    assert instance._progress_payload('running', 10, 10)['percent'] == 99
    assert instance._progress_payload('successful', 10, 10)['percent'] == 100
    assert instance._progress_payload('failed', 10, 10)['percent'] == 100


def test_live_deploy_rows_read_new_intermediate_and_terminal_log_state(playbooks):
    """The automatic state payload updates an existing run as its log grows."""
    root, write = playbooks
    write('vps-update-runtime.yml', [{'import_playbook': 'bulk.yml'}])
    filename = 'vps-update-runtime--run-fixture.log'
    log = root/'data/logs/vps-manager/hosts/fixture'/filename
    log.parent.mkdir(parents=True)
    instance = object.__new__(service.VPSManagerService)
    instance._deploy_progress_cache_lock = threading.Lock()
    instance._deploy_progress_cache = {}
    history = [{'command': 'vps-update-runtime', 'hostnames': ['fixture'],
                'host_logs': {'fixture': {'command': 'vps-update-runtime',
                    'run_id': 'run-fixture', 'filename': filename}}}]
    text = '=== PLAYBOOK RUN START 2026-11-23 10:00:00 ===\n'
    for task in ('Validate PB8', 'Build PB8 wheel', 'Validate PB9', 'Finish PB9'):
        text += f'TASK [{task}]\nok: [fixture]\n'
        log.write_text(text)
        row = instance._build_deploy_progress_rows([], history)[0]
        assert row['task_status'] == 'running'
        assert row['task_step'] == task
        assert row['task_progress']['total'] == 14
        if task != 'Validate PB8':
            assert 0 < row['task_progress']['percent'] < 100
    log.write_text(text+'PLAY RECAP\nfixture : ok=14 changed=3 unreachable=0 failed=0 skipped=0 rescued=0 ignored=0\n')
    row = instance._build_deploy_progress_rows([], history)[0]
    assert row['task_status'] == 'successful'
    assert row['task_progress']['percent'] == 100
