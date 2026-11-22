"""Recover isolated dead process metadata without trusting conflicting live identities."""

import json
import os
from pathlib import Path

import psutil
import pytest

import logging_helpers
import pbcoindata_guard as guard
import pbcoindata_process as processes
from process_identity import ProcessIdentity, write_process_identity


@pytest.fixture
def conflicting_publications(tmp_path, monkeypatch):
    """Model the observed one-second discrepancy using synthetic process observations."""
    pid = 424242
    path = processes.pid_path(tmp_path)
    path.parent.mkdir(parents=True)
    path.write_text(str(pid))
    write_process_identity(path, ProcessIdentity(pid, 1000.0))
    registration = {**guard.scope(tmp_path), **guard.runtime_scope(),
                    "pid": pid, "create_time": 1001.0, "phase": "ready"}
    guard.registration_path(tmp_path).write_text(json.dumps(registration))
    before = {p: p.read_bytes() for p in path.parent.iterdir()}
    original = guard.process_observation
    mode = {"state": "absent", "probes": []}

    def observe(wanted="self"):
        """Never inspect the synthetic referenced PID on the host."""
        if wanted != pid:
            return original(wanted)
        if mode["state"] == "absent":
            raise ProcessLookupError(pid)
        if mode["state"] == "denied":
            raise PermissionError("Synthetic process inspection denied")
        return 1001.0, mode["state"], 100

    class Process:
        """Supply only the creation time consumed by publication classification."""

        def __init__(self, wanted):
            assert wanted == pid

        def create_time(self):
            """Keep psutil and stdlib observations on the same synthetic lifetime."""
            if mode["state"] == "absent":
                raise psutil.NoSuchProcess(pid)
            if mode["state"] == "denied":
                raise psutil.AccessDenied(pid)
            return 1001.0

    def probe(wanted, signum):
        """Permit no delivered signal, and distinguish absence from permission denial."""
        assert wanted == pid and signum == 0
        mode["probes"].append(signum)
        if mode["state"] == "absent":
            raise ProcessLookupError(pid)
        raise PermissionError("Synthetic kernel probe denied")

    monkeypatch.setattr(guard, "process_observation", observe)
    monkeypatch.setattr(processes.psutil, "Process", Process)
    monkeypatch.setattr(guard.os, "kill", probe)
    previous_guards = list(guard._LIFETIME_GUARDS)
    yield tmp_path, mode, before
    for lease in list(guard._LIFETIME_GUARDS):
        if lease not in previous_guards:
            lease.release()
            guard._LIFETIME_GUARDS.remove(lease)


def test_dead_disagreement_is_readable_and_recovers_on_guarded_start(conflicting_publications):
    """The real host's dead-PID discrepancy no longer prevents startup or GUI status."""
    root, mode, before = conflicting_publications
    records = guard.read_publications(root)["records"]
    assert len(records) == 2
    assert {record["create_time"] for record in records} == {1000.0, 1001.0}
    assert all(guard.publication_state(root, record) == "retired" for record in records)
    assert processes.snapshot(root)["status"] == "stopped"
    assert all(path.read_bytes() == content for path, content in before.items())
    lease = guard.acquire_daemon(root)
    assert not processes.pid_path(root).exists()
    assert not processes.identity_path(processes.pid_path(root)).exists()
    current = json.loads(guard.registration_path(root).read_text())
    assert current["pid"] == os.getpid() and current["phase"] == "starting"
    assert lease.fd is not None and mode["probes"]


@pytest.mark.parametrize("state", ["S", "Z", "X", "denied"])
def test_conflicting_live_or_inaccessible_identity_is_never_repaired(conflicting_publications, state):
    """Live, zombie, terminal and inaccessible conflicts retain every ownership fence."""
    root, mode, before = conflicting_publications
    mode["state"] = state
    result = processes.snapshot(root)
    assert result["status"] == "unknown"
    assert "registration and sidecar disagree" in result["error"]
    with pytest.raises(RuntimeError, match="registration and sidecar disagree"):
        guard.acquire_daemon(root)
    assert all(path.read_bytes() == content for path, content in before.items())
    assert mode["probes"] == []


def test_disagreement_requires_kernel_exit_confirmation(conflicting_publications, monkeypatch):
    """A missing proc record alone cannot authorize cleanup when the kernel denies access."""
    root, _, before = conflicting_publications

    def denied(pid, signum):
        """Simulate procfs hiding a process which the kernel cannot confirm absent."""
        assert pid == 424242 and signum == 0
        raise PermissionError("Synthetic probe denied")

    monkeypatch.setattr(guard.os, "kill", denied)
    assert processes.snapshot(root)["status"] == "unknown"
    with pytest.raises(RuntimeError):
        guard.acquire_daemon(root)
    assert all(path.read_bytes() == content for path, content in before.items())


def test_disagreement_cannot_remove_concurrently_changed_metadata(conflicting_publications, monkeypatch):
    """A byte change after retirement confirmation still cancels guarded recovery."""
    root, _, _ = conflicting_publications
    original = guard._check_existing_publications
    path = processes.pid_path(root)

    def changed(checkout):
        """Replace the numeric publication after the original evidence was gathered."""
        files = original(checkout)
        path.write_text("999999")
        return files

    monkeypatch.setattr(guard, "_check_existing_publications", changed)
    with pytest.raises(RuntimeError, match="publications changed"):
        guard.acquire_daemon(root)
    assert path.read_text() == "999999"


def test_service_guard_logs_a_persistent_start_failure_and_exits_78(monkeypatch, tmp_path):
    """Systemd can stop retrying ownership failures while the GUI log retains the cause."""
    messages = []

    def blocked(root):
        """Represent a permanently unconfirmed ownership publication."""
        raise guard.StartupBlockedError("PBCoinData registration and sidecar disagree")

    monkeypatch.setattr(guard, "acquire_daemon", blocked)
    monkeypatch.setattr(logging_helpers, "human_log", lambda *args, **kwargs: messages.append((args, kwargs)))
    with pytest.raises(SystemExit) as result:
        guard.acquire_service_daemon(tmp_path)
    assert result.value.code == 78
    assert "registration and sidecar disagree" in messages[0][0][1]
    assert messages[0][1]["level"] == "ERROR"


@pytest.mark.parametrize("error", [RuntimeError, ValueError, json.JSONDecodeError])
def test_service_guard_keeps_transient_publication_races_retryable(monkeypatch, tmp_path, error):
    """A concurrent publication write must not become a permanent exit-78 failure."""
    def changed(root):
        """Represent changed evidence before the atomic registration write."""
        if error is json.JSONDecodeError:
            raise error("PBCoinData publications changed during registration", "{", 1)
        raise error("PBCoinData publications changed during registration")

    monkeypatch.setattr(guard, "acquire_daemon", changed)
    with pytest.raises(error, match="publications changed"):
        guard.acquire_service_daemon(tmp_path)


def test_service_guard_logging_failure_preserves_reason_and_exit_code(monkeypatch, tmp_path, capsys):
    """An unavailable log destination must not turn a permanent failure into a crash loop."""
    def blocked(root):
        """Keep an unresolved conflict unchanged."""
        raise guard.StartupBlockedError("PBCoinData registration and sidecar disagree")

    def failed_log(*args, **kwargs):
        """Represent a full or inaccessible log filesystem."""
        raise OSError("Synthetic log failure")

    monkeypatch.setattr(guard, "acquire_daemon", blocked)
    monkeypatch.setattr(logging_helpers, "human_log", failed_log)
    with pytest.raises(SystemExit) as result:
        guard.acquire_service_daemon(tmp_path)
    assert result.value.code == 78
    assert "registration and sidecar disagree" in capsys.readouterr().err
