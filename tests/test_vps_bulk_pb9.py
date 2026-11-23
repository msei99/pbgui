"""Bulk runtime updates include confirmed PB9 installs without new installation."""
from contextlib import nullcontext
from itertools import product
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from jinja2 import Environment, StrictUndefined

import vps_manager_core
import vps_manager_service as service_mod
from vps_manager_service import VPSManagerService

COMMANDS = ('vps-update-runtime', 'vps-update-pbgui-runtime')


@pytest.mark.parametrize('command', COMMANDS)
@pytest.mark.parametrize('pb7,pb8', list(product((False, True), repeat=2)))
@pytest.mark.parametrize('role', ['master', 'slave'])
@pytest.mark.parametrize('fresh', [True, False])
def test_bulk_pb9_updates_only_confirmed_installed_runtimes(command, pb7, pb8, role, fresh):
    """Host locks and existing action guards run before one composed playbook."""
    service = object.__new__(VPSManagerService)
    vps = SimpleNamespace(command='', command_text='', command_run_id='test-run',
                          runtime_profile='pb7', user_pw=None,
                          _task_log_path=lambda *_: Path('fixture.log'))
    meta = {'role': role, 'pb8installed': pb8, 'pb9installed': True}
    if pb7:
        meta.update(pb7v='v7.12.0', pb7c='a'*40, pb7py='3.12')
    host_state = {'meta': meta}
    calls = []
    service._host_task_start_lock = lambda _: nullcontext()
    service._deploy_shutdown_requested = lambda: False
    service._require_vps = lambda _: vps
    service._apply_session_secrets_to_vps = lambda *_: None
    service._raise_if_vps_task_active = lambda *_: None
    service._get_monitor_state = lambda: {}
    service._get_host_telemetry = lambda *_: host_state
    service._host_telemetry_fresh = lambda _: fresh
    service._credential_playbook_vars = lambda *_: {'pbgui_role': role}
    service.vpsmanager = SimpleNamespace(update_vps=lambda *args, **kwargs: calls.append(kwargs))
    if not fresh:
        with pytest.raises(ValueError, match='Fresh'):
            service._start_vps_deploy_host('fixture', hostname='fixture', command=command,
                                          debug=False, extra_vars=None)
        assert calls == []
        return
    result = service._start_vps_deploy_host('fixture', hostname='fixture', command=command,
                debug=False, extra_vars={'pbgui_update_pb7': True, 'pbgui_update_pb8': True,
                                         'pbgui_update_pb9': False})
    assert result['command'] == command
    assert len(calls) == 1
    variables = calls[0]['extra_vars']
    assert {k: variables[k] for k in ('pbgui_update_pb7', 'pbgui_update_pb8', 'pbgui_update_pb9')} == {
        'pbgui_update_pb7': pb7, 'pbgui_update_pb8': pb8, 'pbgui_update_pb9': True,
    }
    assert variables['pbgui_role'] == role
    assert ('pb7_min_free_bytes' in variables) == pb7
    assert ('pb8_min_free_bytes' in variables) == pb8


@pytest.mark.parametrize('command', COMMANDS)
@pytest.mark.parametrize('pb9', [False, None])
def test_bulk_does_not_infer_pb9_from_saved_profile(command, pb9):
    """Unconfirmed PB9 stays excluded and old host routing remains intact."""
    resolved = service_mod._profile_aware_vps_update_command(command, 'pb7_pb8',
                   pb7_installed=False, pb8_installed=True, pb9_installed=pb9)
    assert resolved == ('vps-update-pb8' if command == COMMANDS[0] else 'vps-update-pbgui-pb8')


@pytest.mark.parametrize('flags', list(product((False, True), repeat=3)))
def test_composed_playbook_selects_exact_runtime_imports(flags):
    """Evaluate actual import conditions for every installed-generation combination."""
    plays = yaml.safe_load(Path('vps-update-runtime.yml').read_text())
    env = Environment(undefined=StrictUndefined)
    variables = dict(zip(('pbgui_update_pb7', 'pbgui_update_pb8', 'pbgui_update_pb9'), flags))
    # Ansible's bool filter is only used on controller-authored booleans here.
    env.filters['bool'] = bool
    selected = [play['import_playbook'] for play in plays
                if env.compile_expression(play['when'])(**variables)]
    assert selected == [f'vps-update-pb{generation}.yml'
                        for generation, installed in zip((7, 8, 9), flags) if installed]
    assert not any(env.compile_expression(play['when'])() for play in plays)
    combined = yaml.safe_load(Path('vps-update-pbgui-runtime.yml').read_text())
    assert [play['import_playbook'] for play in combined] == [
        'vps-update-pbgui.yml', 'vps-update-runtime.yml',
    ]
    assert vps_manager_core._command_updates_pbgui('vps-update-pbgui-runtime')
    assert not vps_manager_core._command_updates_pbgui('vps-update-runtime')
