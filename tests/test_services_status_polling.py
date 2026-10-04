"""Regression tests for the cost of the polled Services status endpoint (#393)."""

import threading

import api.services as services


def test_status_scans_daemon_processes_once_per_request(monkeypatch) -> None:
    """All stopped legacy services share one OS process scan."""
    scans = []
    monkeypatch.setattr(services, "_status_reconcile_last_ok", float("inf"))
    monkeypatch.setattr(services, "_systemd_service_status", lambda name: None)
    monkeypatch.setattr(services, "_optional_service_blocker", lambda name: None)
    monkeypatch.setattr(services, "_systemd_enable_status", lambda *args: {})
    monkeypatch.setattr(services, "_read_service_pid", lambda name: None)

    def scan():
        scans.append(1)
        return [{"service": "pbrun"}]

    monkeypatch.setattr(services, "_collect_pbgui_daemon_processes", scan)
    result = services.get_status(session=None)
    assert len(scans) == 1
    assert result["pbrun"]["running"] is True
    assert result["pbdata"]["running"] is False


def test_status_throttles_credential_reconciliation(monkeypatch) -> None:
    """Pending credential reconciliation runs at most once per interval after success."""
    calls = []
    clock = [1000.0]
    monkeypatch.setattr(services, "_status_reconcile_last_ok", float("-inf"))
    monkeypatch.setattr(services.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(services, "reconcile_pending_credentials", lambda root: calls.append(root))
    monkeypatch.setattr(services, "_collect_pbgui_daemon_processes", lambda: [])
    monkeypatch.setattr(services, "_service_status", lambda name, daemons=None: {"running": False})

    services.get_status(session=None)
    services.get_status(session=None)
    assert len(calls) == 1
    clock[0] += services._STATUS_RECONCILE_INTERVAL_S
    services.get_status(session=None)
    assert len(calls) == 2


def test_status_retries_failed_credential_reconciliation(monkeypatch) -> None:
    """A failed reconciliation is not treated as done and retries on the next poll."""
    calls = []

    def failing(root):
        calls.append(root)
        raise RuntimeError("locked")

    monkeypatch.setattr(services, "_status_reconcile_last_ok", float("-inf"))
    monkeypatch.setattr(services, "reconcile_pending_credentials", failing)
    monkeypatch.setattr(services, "_collect_pbgui_daemon_processes", lambda: [])
    monkeypatch.setattr(services, "_service_status", lambda name, daemons=None: {"running": False})
    monkeypatch.setattr(services, "_log", lambda *args, **kwargs: None)

    services.get_status(session=None)
    services.get_status(session=None)
    assert len(calls) == 2


def test_concurrent_status_requests_reconcile_once(monkeypatch) -> None:
    """Parallel polls must not both run the credential reconciliation."""
    calls = []
    entered = threading.Event()
    release = threading.Event()

    def slow_reconcile(root):
        calls.append(root)
        entered.set()
        release.wait(5)

    monkeypatch.setattr(services, "_status_reconcile_last_ok", float("-inf"))
    monkeypatch.setattr(services, "reconcile_pending_credentials", slow_reconcile)
    monkeypatch.setattr(services, "_collect_pbgui_daemon_processes", lambda: [])
    monkeypatch.setattr(services, "_service_status", lambda name, daemons=None: {"running": False})

    first = threading.Thread(target=services.get_status, kwargs={"session": None})
    first.start()
    assert entered.wait(5)
    services.get_status(session=None)
    release.set()
    first.join(5)
    assert len(calls) == 1
