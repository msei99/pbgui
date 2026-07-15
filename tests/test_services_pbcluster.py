"""Regression tests for local PBCluster service integration."""

import asyncio
import json
from pathlib import Path
import subprocess
import sys

import pytest

import api.services as services
import PBApiServer


def test_local_services_registry_includes_pbcluster() -> None:
    """Local Services API exposes PBCluster with its systemd unit and PID file."""

    assert "pbcluster" in services._SERVICES
    assert services._SYSTEMD_SERVICE_UNITS["pbcluster"] == "pbgui-pbcluster.service"
    assert services._SERVICE_SCRIPT_NAMES["pbcluster"] == "PBCluster.py"
    assert services._SERVICE_PID_FILES["pbcluster"] == "pbcluster.pid"
    assert "pbcluster" in services._MIGRATION_DEFAULT_SERVICES
    assert "pbcluster" in services._MIGRATION_LEGACY_STOP_SERVICES


def test_local_services_registry_includes_monitor_agent() -> None:
    """Local Services API exposes PBMonitorAgent as a systemd-only service."""

    assert "monitor-agent" in services._SERVICES
    assert services._SYSTEMD_SERVICE_UNITS["monitor-agent"] == "pbgui-monitor-agent.service"
    assert services._SERVICE_SCRIPT_NAMES["monitor-agent"] == "monitor_agent.py"
    assert services._SERVICE_PID_FILES["monitor-agent"] == "pbmonitoragent.pid"
    assert "monitor-agent" in services._MIGRATION_DEFAULT_SERVICES
    assert "monitor-agent" not in services._MIGRATION_LEGACY_STOP_SERVICES


def test_local_services_ui_includes_pbcluster() -> None:
    """Services page renders a PBCluster card/panel and log viewer target."""

    source = Path("frontend/services_monitor.html").read_text(encoding="utf-8")

    assert "data-panel=\"pbcluster\"" in source
    assert "id=\"panel-pbcluster\"" in source
    assert "id=\"log-pbcluster\"" in source
    assert "id: 'pbcluster'" in source
    assert "PBCluster.log" in source


def test_local_services_ui_includes_monitor_agent() -> None:
    """Services page renders PBMonitorAgent controls and log viewer target."""

    source = Path("frontend/services_monitor.html").read_text(encoding="utf-8")

    assert "data-panel=\"monitor-agent\"" in source
    assert "id=\"panel-monitor-agent\"" in source
    assert "id=\"log-monitor-agent\"" in source
    assert "id: 'monitor-agent'" in source
    assert "PBMonitorAgent.log" in source


def test_master_installers_enable_monitor_agent_service() -> None:
    """Local and remote browser installers include the monitor-agent systemd unit."""

    core_source = Path("setup/installer/core.py").read_text(encoding="utf-8")
    remote_source = Path("setup/installer/scripts/remote_master_bootstrap.sh").read_text(encoding="utf-8")

    assert '"pbgui-monitor-agent.service"' in core_source
    assert '"monitor_agent.py"' in core_source
    assert '"api,pbrun,pbdata,pbcoindata,monitor-agent"' in core_source
    assert "--enable api,pbrun,pbdata,pbcoindata,monitor-agent" in remote_source


def test_local_services_ui_uses_real_restart_action() -> None:
    """Services page uses the restart endpoint instead of stop/start timers."""

    source = Path("frontend/services_monitor.html").read_text(encoding="utf-8")

    assert "svcAction(\\'" in source
    assert "\\',\\'restart\\')" in source
    assert "setTimeout(function(){svcAction" not in source


def test_systemd_action_failure_returns_final_status(monkeypatch) -> None:
    """Systemd action errors include final service state for the UI."""

    def fake_run(args: list[str], *, timeout: int = 15) -> subprocess.CompletedProcess[str]:
        if args[0] == "stop":
            return subprocess.CompletedProcess(args, 1, stdout="Job canceled", stderr="")
        raise AssertionError(args)

    monkeypatch.setattr(services, "_systemd_unit_for_service", lambda name: "pbgui-pbdata.service")
    monkeypatch.setattr(services, "_run_user_systemctl", fake_run)
    monkeypatch.setattr(
        services,
        "_systemd_service_status",
        lambda name: {
            "running": True,
            "manager": "systemd",
            "unit": "pbgui-pbdata.service",
            "systemd_state": "active",
            "enabled": True,
            "can_enable": True,
        },
    )

    result = services._systemd_service_action("pbdata", "stop")

    assert result is not None
    assert result["running"] is True
    assert result["action_failed"] is True
    assert "Current state: running (active)." in result["error"]
    assert "restarted automatically" in result["error"]


def test_restart_service_route_dispatches_restart(monkeypatch) -> None:
    """Services API exposes a real restart action endpoint."""

    calls: list[tuple[str, str]] = []

    def fake_action(service: str, action: str) -> dict[str, object]:
        calls.append((service, action))
        return {"running": True, "manager": "systemd"}

    monkeypatch.setattr(services, "_service_action", fake_action)

    assert services.restart_service("pbdata")["running"] is True
    assert calls == [("pbdata", "restart")]


def test_restart_service_route_preserves_api_restart_handler(monkeypatch) -> None:
    """Generic restart route delegates api-server to the dedicated restart handler."""

    monkeypatch.setattr(services, "restart_api_server", lambda session=None: {"ok": True, "message": "Restarting..."})

    assert services.restart_service("api-server") == {"ok": True, "message": "Restarting..."}


def test_api_restart_conflict_preserves_http_409(monkeypatch) -> None:
    """An active mutable operation is a conflict, not an internal server error."""
    import api.vps_manager as vps_manager

    class FakeService:
        def active_vps_deploy_summary(self) -> dict[str, object]:
            return {"active": True, "summary": "node-a Update PBGui (running)"}

    monkeypatch.setattr(vps_manager, "get_service_instance", lambda: FakeService())
    monkeypatch.setattr(PBApiServer, "credential_migration_restart_block_reason", lambda _root: "")

    try:
        services.restart_api_server(session=None)
    except services.HTTPException as exc:
        assert exc.status_code == 409
        assert "node-a Update PBGui" in str(exc.detail)
    else:
        raise AssertionError("Expected restart conflict")


def test_services_restart_uses_shared_pbapi_migration_blocker(monkeypatch) -> None:
    """The Services restart route returns 409 for PBApiServer's persisted migration blocker."""

    async def blocked() -> tuple[bool, str]:
        return True, "Credential migration phase import_publish is active"

    monkeypatch.setattr(PBApiServer, "_restart_block_state", blocked)

    with pytest.raises(services.HTTPException) as exc_info:
        services.restart_api_server(session=None)
    assert exc_info.value.status_code == 409
    assert "Credential migration" in str(exc_info.value.detail)


def test_websocket_protocol_documentation_matches_payload_envelopes() -> None:
    """OpenAPI and guides document the message envelopes emitted by active sockets."""
    source = Path("PBApiServer.py").read_text(encoding="utf-8")
    guide = Path("docs/help/28_pbapiserver_service.md").read_text(encoding="utf-8")
    guide_de = Path("docs/help_de/28_pbapiserver_service.md").read_text(encoding="utf-8")

    assert '`{\\"type\\": \\"jobs\\", \\"data\\": [...], \\"timestamp\\": ...}`' in source
    assert '`{\\"type\\": \\"market_data_status\\"' in source
    for text in (guide, guide_de):
        assert '{"type":"jobs","data":[...],"timestamp":...}' in text
        assert "market_data_status" in text
        assert "/api/backtest-v7/ws/bt7" in text
        assert "/api/optimize-v7/ws/opt7" in text
        assert "/api/vps-manager/ws" in text
        assert "pbgui_session" in text


def test_services_api_restart_uses_transient_systemd_unit(monkeypatch) -> None:
    """Services API restart queues work outside pbgui-api.service's cgroup."""
    calls: list[list[str]] = []

    def fake_run(args, **kwargs):
        calls.append(list(args))
        return subprocess.CompletedProcess(args, 0, stdout="queued", stderr="")

    monkeypatch.setattr(services.subprocess, "run", fake_run)
    ok, output = services._queue_api_systemd_restart("pbgui-api.service")

    assert ok is True
    assert output == "queued"
    assert calls[0][:2] == ["systemd-run", "--user"]
    assert "/bin/bash" in calls[0]
    assert "systemctl --user restart pbgui-api.service" in calls[0][-1]


def test_root_api_restart_uses_transient_systemd_unit(monkeypatch) -> None:
    """Nav restart queues work outside pbgui-api.service's cgroup."""
    calls: list[list[str]] = []

    def fake_run(args, **kwargs):
        calls.append(list(args))
        return subprocess.CompletedProcess(args, 0, stdout="queued", stderr="")

    monkeypatch.setattr(PBApiServer.subprocess, "run", fake_run)
    ok, output = PBApiServer._queue_current_api_systemd_restart()

    assert ok is True
    assert output == "queued"
    assert calls[0][:2] == ["systemd-run", "--user"]
    assert "/bin/bash" in calls[0]
    assert "systemctl --user restart pbgui-api.service" in calls[0][-1]


def test_pbcoindata_start_is_not_blocked_without_cmc_key(monkeypatch) -> None:
    """PBCoinData may run without CMC so exchange mappings can still update."""

    monkeypatch.setattr(services, "load_ini", lambda section, key: "")

    assert services._optional_service_blocker("pbcoindata") == ""


def test_api_systemd_handoff_schedules_delayed_restart(monkeypatch, tmp_path) -> None:
    """Migration schedules an API restart outside the current systemd request."""

    calls: list[list[str]] = []

    def fake_subprocess_run(args, **kwargs) -> subprocess.CompletedProcess:
        calls.append(list(args))
        assert kwargs.get("env") == {"XDG_RUNTIME_DIR": "/run/user/1000"}
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(services, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(services, "_systemd_unit_for_service", lambda name: "pbgui-api.service")
    monkeypatch.setattr(
        services,
        "_systemd_service_status",
        lambda name: {"running": True, "systemd_state": "active", "unit": "pbgui-api.service"},
    )
    monkeypatch.setattr(services, "_systemd_user_env", lambda: {"XDG_RUNTIME_DIR": "/run/user/1000"})
    monkeypatch.setattr(services, "_run_user_systemctl", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("direct restart must not run")))
    monkeypatch.setattr(services.subprocess, "run", fake_subprocess_run)

    message = services._schedule_api_systemd_handoff([])

    assert "API restart scheduled through transient systemd unit" in message
    assert len(calls) == 1
    assert calls[0][0:2] == ["systemd-run", "--user"]
    assert "--collect" in calls[0]
    assert "systemctl --user restart \"$unit\"" in calls[0][-1]
    assert "deactivating" in calls[0][-1]


def test_migration_status_requires_start_sh_cleanup(monkeypatch, tmp_path) -> None:
    """Legacy start.sh keeps migration available so the cleanup can run."""

    pbgui_dir = tmp_path / "pbgui"
    pbgui_dir.mkdir()
    (pbgui_dir / "start.sh").write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    monkeypatch.setattr(services, "PBGDIR", str(pbgui_dir))
    monkeypatch.setattr(services, "_read_legacy_crontab", lambda: {"entries": []})
    monkeypatch.setattr(services, "_collect_pbgui_daemon_processes", lambda: [])
    monkeypatch.setattr(
        services,
        "_migration_systemd_units",
        lambda: [
            {"service": service, "unit": unit, "exists": True, "enabled": True, "active": True, "state": "active"}
            for service, unit in services._SYSTEMD_SERVICE_UNITS.items()
        ],
    )
    monkeypatch.setattr(services, "load_ini", lambda section, key: "")
    monkeypatch.setattr(services, "_detect_pbgui_python", lambda: str(tmp_path / "venv" / "bin" / "python"))

    status = services._migration_status_payload()

    assert status["migration_needed"] is True
    assert status["legacy_start_sh"] == {"path": str(pbgui_dir / "start.sh"), "exists": True}


def test_migration_status_requires_not_ready_default_unit(monkeypatch, tmp_path) -> None:
    """Disabled or inactive default systemd units keep migration available."""

    pbgui_dir = tmp_path / "pbgui"
    pbgui_dir.mkdir()
    monkeypatch.setattr(services, "PBGDIR", str(pbgui_dir))
    monkeypatch.setattr(services, "_read_legacy_crontab", lambda: {"entries": []})
    monkeypatch.setattr(services, "_collect_pbgui_daemon_processes", lambda: [])
    monkeypatch.setattr(
        services,
        "_migration_systemd_units",
        lambda: [
            {"service": service, "unit": unit, "exists": True, "enabled": service != "pbdata", "active": service != "pbdata", "state": "inactive" if service == "pbdata" else "active"}
            for service, unit in services._SYSTEMD_SERVICE_UNITS.items()
        ],
    )
    monkeypatch.setattr(services, "_migration_required_services", lambda pbgdir=None: {"api-server", "pbdata"})
    monkeypatch.setattr(services, "load_ini", lambda section, key: "")
    monkeypatch.setattr(services, "_detect_pbgui_python", lambda: str(tmp_path / "venv" / "bin" / "python"))

    status = services._migration_status_payload()

    assert status["migration_needed"] is True
    assert [row["service"] for row in status["not_ready_default_units"]] == ["pbdata"]


def test_migration_status_ignores_not_ready_unrequired_units(monkeypatch, tmp_path) -> None:
    """Disabled PBRun/PBData units are not migration blockers when no workload needs them."""

    pbgui_dir = tmp_path / "pbgui"
    pbgui_dir.mkdir()
    monkeypatch.setattr(services, "PBGDIR", str(pbgui_dir))
    monkeypatch.setattr(services, "_read_legacy_crontab", lambda: {"entries": []})
    monkeypatch.setattr(services, "_collect_pbgui_daemon_processes", lambda: [])
    monkeypatch.setattr(
        services,
        "_migration_systemd_units",
        lambda: [
            {"service": service, "unit": unit, "exists": True, "enabled": service not in {"pbrun", "pbdata"}, "active": service not in {"pbrun", "pbdata"}, "state": "inactive" if service in {"pbrun", "pbdata"} else "active"}
            for service, unit in services._SYSTEMD_SERVICE_UNITS.items()
        ],
    )
    monkeypatch.setattr(services, "_migration_required_services", lambda pbgdir=None: {"api-server"})
    monkeypatch.setattr(services, "load_ini", lambda section, key: "")
    monkeypatch.setattr(services, "_detect_pbgui_python", lambda: str(tmp_path / "venv" / "bin" / "python"))

    status = services._migration_status_payload()

    assert status["migration_needed"] is False
    assert status["not_ready_default_units"] == []


def test_run_systemd_migration_deletes_legacy_start_sh(monkeypatch, tmp_path) -> None:
    """Successful migration removes the legacy start.sh autostart script."""

    pbgui_dir = tmp_path / "pbgui"
    setup_dir = pbgui_dir / "setup"
    setup_dir.mkdir(parents=True)
    setup_script = setup_dir / "setup_systemd.sh"
    setup_script.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    start_script = pbgui_dir / "start.sh"
    start_script.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    before = {
        "warnings": [],
        "legacy_crontab": {"entries": []},
        "required_services": ["api-server", "pbcluster", "pbrun", "pbdata", "pbcoindata", "monitor-agent"],
        "missing_default_units": [],
        "not_ready_default_units": [],
        "legacy_start_sh": {"path": str(start_script), "exists": True},
    }
    statuses = iter([before, {"warnings": []}])
    calls: list[tuple[str, str]] = []

    monkeypatch.setattr(services, "PBGDIR", str(pbgui_dir))
    monkeypatch.setattr(services, "_migration_status_payload", lambda: next(statuses))
    monkeypatch.setattr(services, "_try_enable_linger", lambda user: {"ok": True})
    monkeypatch.setattr(services, "_detect_pbgui_python", lambda: str(tmp_path / "venv" / "bin" / "python"))
    monkeypatch.setattr(services, "_current_username", lambda: "mani")
    monkeypatch.setattr(services, "_stop_legacy_services", lambda logs: logs.append("stopped legacy"))
    monkeypatch.setattr(services, "_service_action", lambda service, action: calls.append((service, action)) or {"running": True, "manager": "systemd"})
    monkeypatch.setattr(services, "_systemd_unit_for_service", lambda service: "pbgui-api.service")
    monkeypatch.setattr(services, "_remove_legacy_crontab_entries", lambda: {"removed": []})
    monkeypatch.setattr(services, "_schedule_api_systemd_handoff", lambda logs: "handoff scheduled")
    monkeypatch.setattr(services.subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 0, stdout="", stderr=""))

    result = services._run_systemd_migration()

    assert result["ok"] is True
    assert not start_script.exists()
    assert any("Deleted legacy start.sh" in line for line in result["logs"])
    assert calls == [("pbcluster", "restart"), ("pbrun", "restart"), ("pbdata", "restart"), ("pbcoindata", "restart"), ("monitor-agent", "restart")]


def test_worker_restart_uses_single_restart_action() -> None:
    """Workers page uses the restart worker endpoint instead of stop/start timers."""

    source = Path("frontend/services_monitor.html").read_text(encoding="utf-8")

    assert "window.workerAction(workerId, 'restart')" in source
    assert "setTimeout(function () { window.workerAction(workerId, 'start')" not in source


def test_worker_action_route_dispatches_restart(monkeypatch) -> None:
    """Worker restart stops and starts the selected worker on the backend."""

    calls: list[tuple[str, str]] = []

    async def fake_stop(worker_id: str) -> None:
        calls.append(("stop", worker_id))

    async def fake_start(worker_id: str) -> None:
        calls.append(("start", worker_id))

    async def fake_find(worker_id: str) -> dict:
        return {"id": worker_id, "running": True}

    monkeypatch.setattr(services, "_stop_worker", fake_stop)
    monkeypatch.setattr(services, "_start_worker", fake_start)
    monkeypatch.setattr(services, "_find_worker", fake_find)

    result = asyncio.run(services.worker_action("market-data-task", "restart"))

    assert result["ok"] is True
    assert result["worker"]["id"] == "market-data-task"
    assert calls == [("stop", "market-data-task"), ("start", "market-data-task")]


def test_market_data_worker_stop_waits_for_process_exit(monkeypatch) -> None:
    """Stopping Market Data Queue waits until the old worker process exits."""

    running_checks = iter([True, False])
    cleared: list[bool] = []
    killed: list[tuple[int, int]] = []

    monkeypatch.setattr(services, "_TASK_WORKER_STOP_TIMEOUT_S", 2.0)
    monkeypatch.setattr(services, "_wait_for_task_worker_exit", lambda pid: asyncio.sleep(0, result=True))
    monkeypatch.setattr(services.os, "kill", lambda pid, sig: killed.append((pid, sig)))
    monkeypatch.setattr(services, "asyncio", services.asyncio)

    import task_queue

    monkeypatch.setattr(task_queue, "read_worker_pid", lambda: 1234)
    monkeypatch.setattr(task_queue, "is_pid_running", lambda _pid: next(running_checks))
    monkeypatch.setattr(task_queue, "clear_worker_pid", lambda: cleared.append(True))

    asyncio.run(services._stop_worker("market-data-task"))

    assert killed == [(1234, services.signal.SIGTERM)]
    assert cleared == [True]


def test_market_data_worker_stop_timeout_reports_error(monkeypatch) -> None:
    """Stopping Market Data Queue reports a timeout instead of starting over an old PID."""

    monkeypatch.setattr(services, "_TASK_WORKER_STOP_TIMEOUT_S", 1.0)
    monkeypatch.setattr(services, "_wait_for_task_worker_exit", lambda pid: asyncio.sleep(0, result=False))
    monkeypatch.setattr(services.os, "kill", lambda _pid, _sig: None)

    import task_queue

    monkeypatch.setattr(task_queue, "read_worker_pid", lambda: 1234)
    monkeypatch.setattr(task_queue, "is_pid_running", lambda _pid: True)

    try:
        asyncio.run(services._stop_worker("market-data-task"))
    except services.HTTPException as exc:
        assert exc.status_code == 409
        assert "did not stop" in str(exc.detail)
    else:
        raise AssertionError("Expected HTTPException")


def test_live_unsubscribe_awaits_watcher_cleanup() -> None:
    """The final live subscriber must wait until the shared WS client is released."""
    from api import live

    async def scenario() -> None:
        released = asyncio.Event()
        queue = asyncio.Queue()
        key = live._wkey("alice", "positions")

        async def watcher() -> None:
            try:
                await asyncio.sleep(60)
            finally:
                released.set()

        live._watcher_lock = None
        live._watcher_subs[key] = {queue}
        live._watcher_tasks[key] = asyncio.create_task(watcher())
        await asyncio.sleep(0)
        await live._unsubscribe("alice", "positions", queue)

        assert released.is_set()
        assert key not in live._watcher_tasks
        assert key not in live._watcher_subs
        await live.shutdown()

    asyncio.run(scenario())


def test_live_status_reports_value_free_resource_counts(monkeypatch) -> None:
    """The authenticated diagnostic reports counts without user or credential values."""
    from api import live
    from Exchange import Exchange
    from api.auth import SessionToken

    async def scenario() -> None:
        task = asyncio.create_task(asyncio.sleep(60))
        queue_one = asyncio.Queue()
        queue_two = asyncio.Queue()
        live._watcher_lock = None
        live._watcher_tasks["positions:alice"] = task
        live._watcher_subs["positions:alice"] = {queue_one, queue_two}
        monkeypatch.setattr(Exchange, "_private_ws_clients", {"bybit:alice": object()})
        monkeypatch.setattr(Exchange, "_private_ws_owners", {"bybit:alice": {"live_session.positions"}})
        session = SessionToken(token="test", user_id="test", created_at=1, expires_at=2)

        payload = await live.live_status(session)

        assert payload == {
            "max_live_users": 10,
            "watcher_registry_count": 1,
            "watcher_active_count": 1,
            "subscriber_key_count": 1,
            "subscriber_reference_count": 2,
            "private_ws_client_count": 1,
            "private_ws_owner_count": 1,
            "cleanup_task_count": 0,
        }
        assert "alice" not in str(payload)
        await live.shutdown()

    asyncio.run(scenario())


def test_live_stream_revalidates_session_and_cleans_up(monkeypatch) -> None:
    """An expired or revoked SSE session stops and releases all subscriptions."""
    from api import live
    from api.auth import SessionToken

    async def scenario() -> None:
        subscribed = []
        unsubscribed = []
        checks = iter([object(), None])

        async def fake_ensure(user_name, kind, queue, user=None) -> None:
            del user
            subscribed.append((user_name, kind, queue))

        def fake_detach(user_name, kind, queue):
            unsubscribed.append((user_name, kind, queue))
            return None

        monkeypatch.setattr(live, "_ensure_watcher", fake_ensure)
        monkeypatch.setattr(live, "_detach_subscriber", fake_detach)
        monkeypatch.setattr(live, "validate_token", lambda _token: next(checks))
        monkeypatch.setattr(live, "SESSION_CHECK_INTERVAL_SECONDS", 0.01)
        session = SessionToken(token="test", user_id="test", created_at=1, expires_at=2)
        response = await live.live_stream(users="alice", session=session)
        stream = response.body_iterator

        assert "connected" in await anext(stream)
        try:
            await anext(stream)
        except StopAsyncIteration:
            pass
        else:
            raise AssertionError("Revoked SSE session remained open")
        await asyncio.sleep(0)

        assert [(user, kind) for user, kind, _queue in subscribed] == [
            ("alice", "positions"),
            ("alice", "balance"),
        ]
        assert [(user, kind) for user, kind, _queue in unsubscribed] == [
            ("alice", "positions"),
            ("alice", "balance"),
        ]
        await live.shutdown()

    asyncio.run(scenario())


def test_live_stream_disconnect_shields_complete_subscription_cleanup(monkeypatch) -> None:
    """Transport cancellation cannot interrupt cleanup between two shared watchers."""
    from api import live
    from api.auth import SessionToken

    async def scenario() -> None:
        unsubscribed = []

        async def fake_ensure(_user_name, _kind, _queue, user=None) -> None:
            del user
            return None

        def fake_detach(user_name, kind, _queue):
            unsubscribed.append((user_name, kind))
            return None

        monkeypatch.setattr(live, "_ensure_watcher", fake_ensure)
        monkeypatch.setattr(live, "_detach_subscriber", fake_detach)
        monkeypatch.setattr(live, "validate_token", lambda _token: object())
        session = SessionToken(token="test", user_id="test", created_at=1, expires_at=2)
        response = await live.live_stream(users="alice", session=session)
        stream = response.body_iterator
        assert "connected" in await anext(stream)

        pending_read = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        pending_read.cancel()
        await asyncio.gather(pending_read, return_exceptions=True)
        await asyncio.sleep(0)

        assert unsubscribed == [("alice", "positions"), ("alice", "balance")]
        await live.shutdown()

    asyncio.run(scenario())


def test_live_cleanup_collects_client_cycles_after_last_watcher(monkeypatch) -> None:
    """The last detached watcher triggers collection of closed client cycles."""
    from api import live

    async def scenario() -> None:
        collected = []

        async def watcher() -> None:
            return None

        live._watcher_tasks.clear()
        monkeypatch.setattr(live.gc, "collect", lambda: collected.append(True) or 0)
        await live._finish_detached_watchers([asyncio.create_task(watcher())])
        assert collected == [True]

    asyncio.run(scenario())


def test_dashboard_shutdown_drains_shared_stream_tasks() -> None:
    """Dashboard shutdown cancels and awaits every shared stream registry."""
    from api import dashboard

    async def scenario() -> None:
        released = asyncio.Event()

        async def watcher() -> None:
            try:
                await asyncio.sleep(60)
            finally:
                released.set()

        task = asyncio.create_task(watcher())
        dashboard._stream_task_lock = None
        dashboard._ws_position_tasks["alice"] = task
        await asyncio.sleep(0)
        await dashboard.shutdown()

        assert released.is_set()
        assert dashboard._ws_position_tasks == {}

    asyncio.run(scenario())


def test_ohlcv_preload_refuses_reused_pid(tmp_path, monkeypatch) -> None:
    """Stopping a restored preload never signals a PID whose identity changed."""
    from api import pb7_ohlcv_tools

    monkeypatch.setattr(pb7_ohlcv_tools, "PBGDIR", tmp_path)
    job_id = "reusedpid"
    job = {
        "job_id": job_id,
        "status": "running",
        "started_at": 1,
        "finished_at": None,
        "pid": 4242,
        "process_created_at": 1.0,
        "returncode": None,
        "error": None,
        "command": ["python", "ohlcv_download.py"],
        "cwd": str(tmp_path),
        "config_path": str(tmp_path / "config.json"),
        "log_path": str(tmp_path / "preload.log"),
        "state_path": str(tmp_path / "data" / "ohlcv_preload" / f"preload_{job_id}.state.json"),
        "target_end_ms": None,
    }
    with pb7_ohlcv_tools._PRELOAD_LOCK:
        pb7_ohlcv_tools._PRELOAD_JOBS.clear()
        pb7_ohlcv_tools._PRELOAD_JOBS[job_id] = job
        pb7_ohlcv_tools._persist_preload_job_locked(job)
    killed: list[tuple[int, int]] = []
    monkeypatch.setattr(pb7_ohlcv_tools, "_preload_process_identity", lambda _job: False)
    monkeypatch.setattr(pb7_ohlcv_tools.os, "killpg", lambda pid, sig: killed.append((pid, sig)))

    payload = pb7_ohlcv_tools.stop_ohlcv_preload_job(job_id)

    assert killed == []
    assert payload["status"] == "stopped"


def test_ohlcv_preload_worker_persists_real_exit_code(tmp_path, monkeypatch) -> None:
    """The detached supervisor writes an authoritative failed terminal status."""
    from api import ohlcv_preload_worker

    state_path = tmp_path / "preload.state.json"
    state_path.write_text(json.dumps({
        "job_id": "job",
        "status": "launching",
        "command": [sys.executable, "-c", "raise SystemExit(7)"],
        "cwd": str(tmp_path),
        "log_path": str(tmp_path / "preload.log"),
    }), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["ohlcv_preload_worker.py", str(state_path)])

    returncode = ohlcv_preload_worker.main()
    state = json.loads(state_path.read_text(encoding="utf-8"))

    assert returncode == 7
    assert state["status"] == "error"
    assert state["returncode"] == 7


def test_lifespan_startup_failure_still_runs_all_shutdown_hooks(monkeypatch, tmp_path) -> None:
    """A partial API startup failure must not bypass unrelated resource cleanup."""
    import api.auth
    import api.v7_instances
    import api.vps
    import master.async_logs
    import master.async_monitor

    calls: list[str] = []
    migration_errors: list[str] = []
    monkeypatch.setattr(PBApiServer, "PBGDIR", str(tmp_path))

    class FakeMonitor:
        def __init__(self):
            self.pool = object()

        async def start(self):
            calls.append("monitor-start")

        async def stop(self):
            calls.append("monitor-stop")

    monkeypatch.setattr(PBApiServer, "_setup_api_logging", lambda: None)
    monkeypatch.setattr(PBApiServer, "load_ini", lambda *_args: "")
    monkeypatch.setattr(PBApiServer, "harden_sensitive_paths", lambda *_args: None)
    monkeypatch.setattr(
        PBApiServer,
        "run_startup_migrations",
        lambda *_args: {"skipped": False, "completed": []},
    )

    def fail_credential_migration(*_args):
        raise RuntimeError("credential recovery required")

    monkeypatch.setattr(PBApiServer, "run_credential_migration", fail_credential_migration)
    monkeypatch.setattr(
        PBApiServer,
        "persist_credential_migration_error",
        lambda reason, *_args: migration_errors.append(reason),
    )
    monkeypatch.setattr(master.async_monitor, "VPSMonitor", FakeMonitor)
    monkeypatch.setattr(master.async_logs, "AsyncLogStreamer", lambda _pool: object())
    monkeypatch.setattr(api.vps, "init", lambda *_args: None)
    monkeypatch.setattr(api.v7_instances, "init", lambda *_args: None)
    monkeypatch.setattr(api.auth, "cleanup_expired_tokens", lambda: 0)
    monkeypatch.setattr(PBApiServer, "db_tools_init", lambda _monitor: None)
    monkeypatch.setattr(PBApiServer, "bt7_startup", lambda: (_ for _ in ()).throw(RuntimeError("startup failed")))

    async def fake_shutdown(name: str) -> None:
        calls.append(name)

    for name in (
        "auth_shutdown", "live_shutdown", "dashboard_shutdown", "heatmap_shutdown",
        "pareto_explorer_shutdown", "coin_data_shutdown", "vps_manager_shutdown",
        "cluster_shutdown", "db_tools_shutdown", "bt7_shutdown", "opt7_shutdown",
    ):
        monkeypatch.setattr(PBApiServer, name, lambda name=name: fake_shutdown(name))

    async def scenario() -> None:
        try:
            async with PBApiServer._lifespan(PBApiServer.app):
                pass
        except RuntimeError as exc:
            assert str(exc) == "startup failed"
        else:
            raise AssertionError("Expected startup failure")

    asyncio.run(scenario())

    assert "monitor-stop" in calls
    assert migration_errors == ["RuntimeError: credential recovery required"]
    for name in (
        "auth_shutdown", "live_shutdown", "dashboard_shutdown", "heatmap_shutdown",
        "pareto_explorer_shutdown", "coin_data_shutdown", "vps_manager_shutdown",
        "cluster_shutdown", "db_tools_shutdown", "bt7_shutdown", "opt7_shutdown",
    ):
        assert name in calls
