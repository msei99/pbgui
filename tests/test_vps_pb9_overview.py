"""PB9 VPS overview fields use only isolated host facts and cached upstream data."""
from types import SimpleNamespace

import pytest

import vps_manager_service as module


@pytest.fixture
def overview(monkeypatch):
    """Construct the real row builders without services, runtime files or SSH."""
    service = object.__new__(module.VPSManagerService)
    service._get_upstream_release_snapshot = lambda: {'repositories': {
        'pb8': {'state': 'ok', 'target_commit': '8' * 40, 'heads': {'master': '8' * 40}},
        'pb9': {'state': 'ok', 'default_branch': 'v9', 'target_commit': '9' * 40, 'heads': {'v9': '9' * 40}},
    }}
    service._get_local_agent_contract = lambda: ({}, {})
    service._get_remote_agent_contract = lambda state: ({}, {})
    service._host_meta = lambda state: state.get('meta', {})
    service._host_online = lambda state: True
    service._host_telemetry_fresh = lambda state: True
    service._host_telemetry_age = lambda state: 0.0
    service._build_remote_rtd = lambda state: 0
    service.vpsmanager = SimpleNamespace(find_vps_by_hostname=lambda name: None)
    monkeypatch.setattr(module, '_local_master_name', lambda: 'controller')
    monkeypatch.setattr(module.psutil, 'boot_time', lambda: 0)
    return service


@pytest.mark.parametrize('blocked,wheel', [(False, True), (False, False), (True, True)])
def test_master_and_remote_rows_share_pb9_fields(overview, blocked, wheel):
    """Both paths display the same version, branch, update and runtime warnings."""
    meta = {'pb9v': 'v9.0.0.dev0', 'pb9py': '3.12.3', 'pb9b': 'HEAD', 'pb9c': '9' * 40,
            'pb9installed': True, 'pb9ready': True, 'pb9wheel': wheel,
            'pb9blocked': blocked, 'pb9reason': 'Update PB9' if blocked else ''}
    overview._get_local_host_meta = lambda: meta
    master = overview._build_master_overview_row()
    remote = overview._build_vps_overview_row('worker', {'meta': meta}, known_host_keys={})
    fields = {k: v for k, v in master.items() if k.startswith('pb9')}
    assert fields == {k: v for k, v in remote.items() if k.startswith('pb9')}
    assert master['pb9'] == 'v9.0.0.dev0 /3.12.3'
    assert master['pb9_branch'] == 'v9 (9999999)'
    assert master['pb9_github'] == '✅'
    assert master['pb9_runtime_ready'] is (not blocked)
    assert master['pb9_wheel_required'] is (not wheel)


def test_pb9_does_not_compare_with_pb8_head_and_missing_metadata_is_safe(overview):
    """Package versions and PB8 heads cannot supply a positive PB9 update signal."""
    fields = overview._build_pb9_overview_fields({'pb9c': '8' * 40, 'pb9installed': True})
    assert fields['pb9_github'].startswith('❌')
    missing = overview._build_pb9_overview_fields({})
    assert missing['pb9_installed'] is False
    assert missing['pb9_runtime_ready'] is False
    assert missing['pb9_wheel_required'] is False
    blocked = overview._build_pb9_overview_fields({'pb9blocked': True})
    assert blocked['pb9_runtime_reason'] == 'PB9 runtime unavailable; run Update PB9'


def test_pb9_running_bots_are_counted_once_per_generation(overview):
    """PB9 snapshots contribute without duplicating their existing telemetry row."""
    state = {'meta': {}, 'instances': [{'p': '9', 'u': 'same'}],
             'v9_instances': [{'name': 'same', 'running': True}, {'name': 'off', 'running': False}],
             'v8_instances': [{'name': 'same', 'running': True}]}
    row = overview._build_vps_overview_row('worker', state, known_host_keys={})
    assert row['running_bots'] == 2
