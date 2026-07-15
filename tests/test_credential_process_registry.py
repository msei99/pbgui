"""Process-level credential protocol barrier regression coverage."""

from __future__ import annotations

import itertools
import json
import os
from pathlib import Path

import psutil
import pytest

import credential_process_registry as registry


SERVICES = (
    "PBApiServer",
    "PBCluster",
    "PBCoinData",
    "PBRun",
    "Market Data worker",
    "TradFi Sync",
    "PBMonitorAgent",
)


def _write_registry(root: Path, states: tuple[str, ...], *, now: float = 100.0) -> list[dict[str, object]]:
    """Write v2 entries for new services and return all non-absent processes."""
    (root / "api").mkdir(parents=True, exist_ok=True)
    (root / "api" / "serial.txt").write_text("42\n", encoding="utf-8")
    path, _lock = registry._registry_paths(root)
    entries: dict[str, dict[str, object]] = {}
    processes: list[dict[str, object]] = []
    for index, (service, state) in enumerate(zip(SERVICES, states, strict=True), start=10):
        if state == "absent":
            continue
        process = {"pid": index, "create_time": float(index), "service": service}
        processes.append(process)
        if state == "new":
            entries[f"{index}:{float(index):.6f}"] = {
                **process,
                "credential_protocol_version": 2,
                "code_serial": "42",
                "capability_generation": 1,
                "heartbeat_monotonic": now,
                "updated_at": "2026-07-15T00:00:00+00:00",
            }
    registry._write_registry(path, {"version": 1, "entries": entries})
    return processes


@pytest.mark.parametrize("states", itertools.product(("old", "new", "absent"), repeat=len(SERVICES)))
def test_every_service_process_permutation_is_automatic_and_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    states: tuple[str, ...],
) -> None:
    """Every live old service blocks while stopped services never require sequencing."""
    monkeypatch.setattr(registry, "_process_matches", lambda _pid, _created: True)
    processes = _write_registry(tmp_path, states)

    result = registry.process_barrier_readiness(tmp_path, processes=processes, now=100.0)

    expected_waiting = sorted(
        service for service, state in zip(SERVICES, states, strict=True) if state == "old"
    )
    assert result["ready"] is (not expected_waiting)
    assert result["waiting_services"] == expected_waiting
    assert all("pid" not in item and "command" not in item for item in result["services"])


def test_arbitrary_restart_order_auto_completes_without_losing_service_availability(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each restart removes one blocker and the last restart opens the barrier automatically."""
    monkeypatch.setattr(registry, "_process_matches", lambda _pid, _created: True)
    order = tuple(reversed(SERVICES))
    states = ["old"] * len(SERVICES)
    for service in order:
        before = registry.process_barrier_readiness(
            tmp_path,
            processes=_write_registry(tmp_path, tuple(states)),
            now=100.0,
        )
        assert service in before["waiting_services"]
        states[SERVICES.index(service)] = "new"
        after = registry.process_barrier_readiness(
            tmp_path,
            processes=_write_registry(tmp_path, tuple(states)),
            now=100.0,
        )
        assert service not in after["waiting_services"]
        assert len(after["services"]) == states.count("new")
    assert after["ready"] is True


@pytest.mark.parametrize("field,value", [
    ("create_time", 99.0),
    ("credential_protocol_version", 1),
    ("capability_generation", 2),
    ("heartbeat_monotonic", 69.0),
])
def test_stale_or_mismatched_capability_never_satisfies_live_process(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: object,
) -> None:
    """PID reuse, stale heartbeat, old protocol, and generation all fail closed."""
    monkeypatch.setattr(registry, "_process_matches", lambda _pid, _created: True)
    processes = _write_registry(tmp_path, ("new",) + ("absent",) * (len(SERVICES) - 1))
    path, _lock = registry._registry_paths(tmp_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    record = next(iter(payload["entries"].values()))
    record[field] = value
    registry._write_registry(path, payload)

    result = registry.process_barrier_readiness(tmp_path, processes=processes, now=100.0)

    assert result == {"ready": False, "services": [], "waiting_services": ["PBApiServer"]}


def test_registry_is_owner_only_and_unregisters_cleanly(tmp_path: Path) -> None:
    """The live registry uses private permissions and optional orderly cleanup."""
    (tmp_path / "api").mkdir()
    (tmp_path / "api" / "serial.txt").write_text("42\n", encoding="utf-8")
    registry.register_process_capability(tmp_path, "PBApiServer")
    path, _lock = registry._registry_paths(tmp_path)
    assert path.stat().st_mode & 0o777 == 0o600
    assert json.loads(path.read_text(encoding="utf-8"))["entries"]
    registry.unregister_process_capability(tmp_path)
    assert json.loads(path.read_text(encoding="utf-8"))["entries"] == {}


def test_unrelated_serial_update_does_not_close_protocol_barrier(tmp_path: Path) -> None:
    """A global API serial bump does not invalidate an unchanged protocol capability."""
    (tmp_path / "api").mkdir()
    serial_path = tmp_path / "api" / "serial.txt"
    serial_path.write_text("42\n", encoding="utf-8")
    heartbeat = registry.ProcessCapabilityHeartbeat(tmp_path, "PBApiServer")
    heartbeat.__enter__()
    try:
        serial_path.write_text("43\n", encoding="utf-8")
        process = psutil.Process(os.getpid())
        result = registry.process_barrier_readiness(
            tmp_path,
            processes=[{
                "pid": process.pid,
                "create_time": process.create_time(),
                "service": "PBApiServer",
            }],
        )
        assert result["ready"] is True
        assert result["waiting_services"] == []
        assert result["services"][0]["code_serial"] == "42"
    finally:
        heartbeat.close()


def test_process_detection_accepts_only_validated_root_script_paths(tmp_path: Path) -> None:
    """Unrelated same-name paths cannot be classified as PBGui-owned services."""
    (tmp_path / "PBApiServer.py").write_text("", encoding="utf-8")
    assert registry._service_for_process(tmp_path, ["python", str(tmp_path / "PBApiServer.py")]) == "PBApiServer"
    assert registry._service_for_process(tmp_path, ["python", "/tmp/PBApiServer.py"]) == ""
    assert registry._service_for_process(tmp_path, ["python", "not-a-service.py"]) == ""


def test_process_detection_recognizes_validated_uvicorn_api_app(tmp_path: Path) -> None:
    """Supported Uvicorn app launches remain visible to the process barrier."""
    (tmp_path / "PBApiServer.py").write_text("", encoding="utf-8")

    assert registry._service_for_process(
        tmp_path,
        ["uvicorn", "PBApiServer:app"],
        cwd=tmp_path,
    ) == "PBApiServer"
    assert registry._service_for_process(
        tmp_path,
        ["python", "-m", "uvicorn", "--app-dir", str(tmp_path), "PBApiServer:app"],
        cwd=tmp_path.parent,
    ) == "PBApiServer"
    assert registry._service_for_process(
        tmp_path,
        ["uvicorn", "PBApiServer:app"],
        cwd=tmp_path.parent,
    ) == ""


def test_monitor_agent_registration_name_matches_discovery(tmp_path: Path) -> None:
    """Monitor Agent heartbeat and process discovery use one service identity."""
    (tmp_path / "monitor_agent.py").write_text("", encoding="utf-8")

    assert registry._service_for_process(
        tmp_path,
        ["python", str(tmp_path / "monitor_agent.py")],
    ) == "PBMonitorAgent"
