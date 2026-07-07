"""Regression tests for local PBCluster service integration."""

import asyncio
from pathlib import Path
import subprocess

import api.services as services


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
