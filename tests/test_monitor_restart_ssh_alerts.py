"""Monitor startup reconnects must not fabricate SSH outages or recovery events."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

import master.async_monitor as module
from master.async_monitor import AlertRecord, VPSMonitor


@pytest.fixture
def monitor(monkeypatch):
    """Build an isolated alert engine with no config reads, files, SSH or Telegram."""
    clock = [100.0]
    monkeypatch.setattr(module.time, 'monotonic', lambda: clock[0])
    value = object.__new__(VPSMonitor)
    value._startup_ssh_pending = {'fixture-host': 160.0}
    value._alerts = {}
    value._monitor_config = SimpleNamespace(**module.MONITOR_DEFAULTS)
    value._load_alert_routes = lambda: None
    value._update_cpu_threshold_state = lambda _config: None
    value._prune_alert_history = lambda _now: False
    value._save_alert_state = lambda: None
    value._optional_service_expected = lambda *_args: True
    value.store = SimpleNamespace(system={}, instances={}, services={}, streams={},
                                  changed=asyncio.Event())
    connections = {'fixture-host': {'status': 'disconnected', 'last_error': 'initial timeout'}}
    value.pool = SimpleNamespace(get_status_summary=lambda: {'connections': connections})
    events = []

    async def problem(alert):
        """Capture outbound problem intent without delivering any notification."""
        events.append(('problem', alert.host, alert.kind))

    async def recovery(alert):
        """Capture outbound recovery intent without delivering any notification."""
        events.append(('recovery', alert.host, alert.kind))

    value._emit_problem_event = problem
    value._emit_recovery_event = recovery
    return value, connections, clock, events


@pytest.mark.parametrize('status', ['disconnected', 'connecting'])
def test_restart_timeout_then_reconnect_creates_no_alert_or_recovery(monitor, status):
    """Initial SSH attempts remain visible in connection state without new alarm episodes."""
    value, connections, clock, events = monitor
    connections['fixture-host']['status'] = status
    asyncio.run(value._sync_live_alerts())
    assert value._alerts == {}
    assert events == []
    assert connections['fixture-host']['status'] == status
    clock[0] = 140.0
    connections['fixture-host']['status'] = 'connected'
    asyncio.run(value._sync_live_alerts())
    assert value._alerts == {}
    assert events == []
    assert value._startup_ssh_pending == {}


def test_persistent_startup_failure_alerts_at_deadline_once(monitor):
    """The fixed reconnect interval cannot indefinitely mute a real unreachable host."""
    value, _connections, clock, events = monitor
    for now in (120.0, 159.99):
        clock[0] = now
        asyncio.run(value._sync_live_alerts())
    assert events == []
    clock[0] = 160.0
    asyncio.run(value._sync_live_alerts())
    asyncio.run(value._sync_live_alerts())
    assert events == [('problem', 'fixture-host', module.ALERT_KIND_OFFLINE)]
    assert value._startup_ssh_pending == {}
    assert any(alert.active for alert in value._alerts.values())


def test_established_connection_loss_is_not_delayed_by_startup_grace(monitor):
    """A connection that was healthy after startup loses its grace immediately."""
    value, connections, clock, events = monitor
    connections['fixture-host']['status'] = 'connected'
    asyncio.run(value._sync_live_alerts())
    clock[0] = 101.0
    connections['fixture-host']['status'] = 'disconnected'
    asyncio.run(value._sync_live_alerts())
    assert events == [('problem', 'fixture-host', module.ALERT_KIND_OFFLINE)]
    connections['fixture-host']['status'] = 'connected'
    asyncio.run(value._sync_live_alerts())
    assert events[-1] == ('recovery', 'fixture-host', module.ALERT_KIND_OFFLINE)


def test_auth_failure_bypasses_reconnect_grace(monitor):
    """A rejected login is a real actionable error, even during monitor startup."""
    value, connections, _clock, events = monitor
    connections['fixture-host']['status'] = 'auth_failed'
    asyncio.run(value._sync_live_alerts())
    assert events == [('problem', 'fixture-host', module.ALERT_KIND_OFFLINE)]
    assert not value._startup_ssh_pending


def test_other_hosts_and_service_alarms_remain_live(monitor):
    """Startup suppression is per host and does not mute existing monitoring."""
    value, connections, _clock, events = monitor
    connections['other-host'] = {'status': 'disconnected'}
    connections['healthy-host'] = {'status': 'connected'}
    value.store.services['healthy-host'] = {'PBRun': {'status': 'stopped', 'expected': True}}
    asyncio.run(value._sync_live_alerts())
    assert ('problem', 'other-host', module.ALERT_KIND_OFFLINE) in events
    assert ('problem', 'healthy-host', module.ALERT_KIND_SERVICE) in events
    assert not any(host == 'fixture-host' for _event, host, _kind in events)


@pytest.mark.parametrize('kind,name', [(module.ALERT_KIND_OFFLINE, 'offline'),
                                     (module.ALERT_KIND_SERVICE, 'PBRun')])
def test_existing_alerts_are_retained_until_actual_reconnection(monitor, kind, name):
    """A pre-existing outage never falsely recovers just because monitoring restarted."""
    value, connections, _clock, events = monitor
    alert = AlertRecord(id='existing-alert', kind=kind, host='fixture-host', name=name,
                        severity='critical', summary='Existing outage', details='Fixture',
                        active=True, acknowledged=True, episode=3)
    value._alerts[alert.id] = alert
    asyncio.run(value._sync_live_alerts())
    assert alert.active
    assert alert.acknowledged
    assert alert.episode == 3
    assert events == []
    connections['fixture-host']['status'] = 'connected'
    asyncio.run(value._sync_live_alerts())
    assert not alert.active
    assert events == [('recovery', 'fixture-host', kind)]


def test_restart_grace_is_not_applied_outside_startup(monitor):
    """Normal runtime losses are immediate; this is not a general offline delay."""
    value, _connections, _clock, events = monitor
    value._startup_ssh_pending.clear()
    asyncio.run(value._sync_live_alerts())
    assert events == [('problem', 'fixture-host', module.ALERT_KIND_OFFLINE)]


def test_starting_metrics_marks_success_before_next_alert_poll(monitor):
    """The real reconnect path ends grace when its first connected stream starts."""
    value, _connections, _clock, _events = monitor
    value._stream_tasks = {}
    value._stream_generations = {}
    value._stream_started_at = {}

    async def metrics(_host, _generation):
        """Provide a non-I/O stream whose lifecycle stays within this isolated test."""
        await asyncio.Future()

    value._metrics_stream = metrics
    async def scenario():
        """Join the owned fake stream without leaving background work behind."""
        value._start_metrics_stream('fixture-host')
        assert not value._startup_ssh_pending
        task = value._stream_tasks['fixture-host']
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    asyncio.run(scenario())


def test_real_start_stop_owns_reconnect_grace_without_delaying_restart(monitor, monkeypatch, tmp_path):
    """Lifecycle creates one bounded baseline, starts normally and clears it on shutdown."""
    value, _connections, clock, events = monitor
    value._startup_ssh_pending = {'obsolete-host': 999999.0}
    value._running = False
    value.loop = None
    value._host_meta_lifecycle_generation = 0
    value._host_meta_blocked_hosts = set()
    value._host_meta_tasks = set()
    value._tasks = []
    value._stream_tasks = {}
    value._config_changed = asyncio.Event()
    value._enabled_hosts = {'fixture-host'}
    value._ini_watcher = SimpleNamespace(
        _ini_path=tmp_path / 'unused.ini', bind_asyncio=lambda *_a: None,
        start=lambda: None, stop=lambda: None, unbind_asyncio=lambda: None)
    value._build_config_candidate = lambda _snapshot: object()
    value._commit_config_candidate = lambda _candidate: None
    value._load_alert_state = lambda: None
    monkeypatch.setattr(module, 'load_ini_snapshot', lambda _path: {})
    monkeypatch.setattr(module, '_log', lambda *_args, **_kwargs: None)
    value._host_metric_history = {}
    value._bot_metric_history = {}
    value._bot_count_history = {}
    history = SimpleNamespace(load=lambda: None, maybe_flush=lambda **_kw: None)
    value._bot_cpu_history = history
    value._bot_pnl_history = history
    disconnected = []

    async def noop():
        """Stand in for an independent upstream collector without file/network access."""

    async def connect(hosts):
        """Assert grace exists before the first real lifecycle connect attempt."""
        assert hosts == {'fixture-host'}
        assert value._startup_ssh_pending == {'fixture-host': clock[0] + module.SSH_STARTUP_ALERT_GRACE_SECONDS}
        return {'fixture-host': False}

    async def disconnect():
        """Record controlled shutdown without connecting to a host."""
        disconnected.append(True)

    async def dormant_loop():
        """Own an idle task until the real stop lifecycle cancels and joins it."""
        await asyncio.Future()

    value.pool.load_vps_configs = lambda: None
    value.pool.hostnames = lambda: ['fixture-host']
    value.pool.connect_enabled = connect
    value.pool.disconnect_all = disconnect
    value.upstream_releases = SimpleNamespace(start=noop, stop=noop)
    value._main_loop = dormant_loop
    value._hl_expiry_loop = dormant_loop

    async def scenario():
        """No sleep or grace timer is awaited by start/stop."""
        await value.start()
        assert value._running
        assert len(value._tasks) == 2
        assert not events
        await value.stop()
        assert not value._startup_ssh_pending
        assert not value._tasks
        assert disconnected == [True]
        assert value.loop is None
        assert not events
    asyncio.run(scenario())
