"""PBData control checks use mocked systemctl and process identities exclusively."""

from types import SimpleNamespace
from unittest.mock import Mock

import psutil
import pytest

import db_maintenance as maintenance
from database_lock import DatabaseBusyError
from secure_files import atomic_write_private_text


@pytest.mark.parametrize("result", [
    SimpleNamespace(returncode=1, stdout=""),
    SimpleNamespace(returncode=0, stdout="LoadState=loaded\nActiveState=deactivating\n"),
    SimpleNamespace(returncode=0, stdout="unexpected response"),
])
def test_unknown_service_state_is_not_already_stopped(tmp_path, monkeypatch, result):
    """An unverifiable service state prevents any legacy stop or DB mutation."""
    run = Mock(return_value=result)
    monkeypatch.setattr(maintenance.subprocess, "run", run)
    control = maintenance.PBDataControl(tmp_path)
    legacy = Mock(side_effect=AssertionError("Unknown service state must fail first"))
    monkeypatch.setattr(control, "_legacy", legacy)
    with pytest.raises(DatabaseBusyError):
        control.inspect()
    legacy.assert_not_called()
    assert run.call_count == 1


def test_failed_systemd_stop_does_not_claim_success(tmp_path, monkeypatch):
    """A failed stop command propagates before probing/signalling any legacy process."""
    monkeypatch.setattr(maintenance.subprocess, "run", Mock(return_value=SimpleNamespace(returncode=1)))
    control = maintenance.PBDataControl(tmp_path)
    legacy = Mock()
    monkeypatch.setattr(control, "_legacy", legacy)
    with pytest.raises(DatabaseBusyError, match="service stop failed"):
        control.stop("systemd")
    legacy.assert_not_called()


def test_already_stopped_writer_has_no_restart_side_effect(tmp_path, monkeypatch):
    """A positively stopped target retains the none ownership marker."""
    control = maintenance.PBDataControl(tmp_path)
    monkeypatch.setattr(control, "_service_state", lambda: "inactive")
    monkeypatch.setattr(control, "_legacy", lambda: None)
    run = Mock(side_effect=AssertionError("No process operation is needed"))
    monkeypatch.setattr(maintenance.subprocess, "run", run)
    assert control.inspect() == "none"
    control.stop("none")
    control.start("none")
    run.assert_not_called()


def test_wrong_legacy_pid_identity_is_never_signalled(tmp_path, monkeypatch):
    """A PID file cannot authorize stopping a process from another installation."""
    atomic_write_private_text(tmp_path / "data" / "pid" / "pbdata.pid", "12345")
    process = Mock(cmdline=Mock(return_value=["python", "/other/PBData.py"]),
                   cwd=Mock(return_value=str(tmp_path)))
    monkeypatch.setattr(psutil, "Process", Mock(return_value=process))
    control = maintenance.PBDataControl(tmp_path)
    monkeypatch.setattr(control, "_service_state", lambda: "inactive")
    with pytest.raises(DatabaseBusyError, match="PID identity"):
        control.stop("legacy")
    process.terminate.assert_not_called()
    process.kill.assert_not_called()


def test_legacy_stop_waits_for_actual_termination(tmp_path, monkeypatch):
    """Sending a signal without confirmed process exit does not admit maintenance."""
    control = maintenance.PBDataControl(tmp_path)
    process = Mock(wait=Mock(side_effect=psutil.TimeoutExpired(60)))
    monkeypatch.setattr(control, "_legacy", Mock(return_value=process))
    with pytest.raises(DatabaseBusyError, match="did not terminate"):
        control.stop("legacy")
    process.terminate.assert_called_once()
    process.wait.assert_called_once_with(timeout=60)
    process.kill.assert_not_called()


def test_systemd_stop_requires_inactive_postcondition(tmp_path, monkeypatch):
    """Even a successful systemctl command is insufficient when the writer remains active."""
    control = maintenance.PBDataControl(tmp_path)
    monkeypatch.setattr(maintenance.subprocess, "run", Mock(return_value=SimpleNamespace(returncode=0)))
    monkeypatch.setattr(control, "_legacy", lambda: None)
    monkeypatch.setattr(control, "_service_state", lambda: "active")
    with pytest.raises(DatabaseBusyError, match="still running"):
        control.stop("systemd")
