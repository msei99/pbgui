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


def test_local_services_ui_includes_pbcluster() -> None:
    """Services page renders a PBCluster card/panel and log viewer target."""

    source = Path("frontend/services_monitor.html").read_text(encoding="utf-8")

    assert "data-panel=\"pbcluster\"" in source
    assert "id=\"panel-pbcluster\"" in source
    assert "id=\"log-pbcluster\"" in source
    assert "id: 'pbcluster'" in source
    assert "PBCluster.log" in source


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
