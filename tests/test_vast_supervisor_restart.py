"""Offline supervisor discovery and explicit restart scheduling regressions."""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import vast_supervisor_restart as restart


@pytest.mark.parametrize('record,expected', [({}, True),
    ({'pid': 42, 'created_at': 12.0, 'code_serial': '6'}, True),
    ({'pid': 42, 'created_at': 12.0, 'code_serial': '7'}, False),
    ({'pid': 42, 'created_at': 11.0, 'code_serial': '7'}, True)])
def test_discovery_checks_startup_serial_and_exact_process(tmp_path, monkeypatch, record, expected):
    """Legacy, outdated and recycled process identities require a new supervisor."""
    identifier = 'a' * 32
    unit = f'pbgui-vast-{identifier}-run.service'
    state = tmp_path / 'data/vast/jobs' / identifier / 'state.json'
    state.parent.mkdir(parents=True)
    state.write_text(json.dumps({'supervisor_run': record}))
    process = SimpleNamespace(pid=42, info={'pid':42, 'create_time':12.0,
        'cmdline':['python',str(tmp_path / 'vast_job_runner.py'),'run',identifier]})
    monkeypatch.setattr(restart.psutil, 'process_iter', lambda *_: [process])
    read = Path.read_text
    monkeypatch.setattr(Path, 'read_text', lambda p,*a,**k: '0::/user.slice/'+unit if str(p)=='/proc/42/cgroup' else read(p,*a,**k))
    rows = restart.stale_supervisors(tmp_path, '7')
    assert bool(rows) is expected
    if rows:
        assert rows[0]['unit'] == unit
    monkeypatch.setattr(Path, 'read_text', lambda p,*a,**k: '0::/unrelated.service' if str(p)=='/proc/42/cgroup' else read(p,*a,**k))
    assert restart.stale_supervisors(tmp_path, '7') == []


def test_restart_button_lists_legacy_supervisor_and_schedules_before_api(monkeypatch):
    """Confirmed restart includes verified cloud units and never sends rental commands."""
    import PBApiServer as api

    unit = 'pbgui-vast-' + 'a'*32 + '-run.service'
    row = {'unit':unit,'label':'Vast run supervisor aaaaaaaa','service':'VastSupervisor'}
    monkeypatch.setattr(api, '_vast_supervisor_restart_state', lambda: [row])
    monkeypatch.setattr(api, '_runtime_service_restart_state', lambda: {})
    monkeypatch.setattr(api, '_read_serial', lambda: 7)
    monkeypatch.setattr(api, '_startup_serial', 7)
    monkeypatch.setattr(api, '_runtime_restart_reasons', [])
    payload = api._restart_status_payload()
    assert payload['needs_restart'] and payload['service_restart_required']
    assert payload['restart_services'] == [row]
    commands = []
    def run(args, **kwargs):
        """Capture transient unit creation without invoking systemd."""
        commands.append(args)
        return SimpleNamespace(returncode=0,stdout='',stderr='')
    monkeypatch.setattr(api.subprocess, 'run', run)
    ok, _ = api._queue_current_api_systemd_restart([unit])
    assert ok
    script = commands[-1][-1]
    assert script.index(unit) < script.index('systemctl --user restart ' + api._API_SYSTEMD_UNIT)
    assert 'ssh ' not in script and 'cleanup' not in script
    assert not api._queue_current_api_systemd_restart(['pbgui-vast-'+'b'*32+'-run.service'])[0]
