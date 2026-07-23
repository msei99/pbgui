"""Isolated contract tests for monitor-agent caches and master consumption."""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import logging_helpers
import monitor_agent
import master.async_monitor as monitor_mod
from master.async_monitor import MonitorAgentPayloadError, VPSMonitor
from master.async_pool import AsyncSSHPool, ConnectionAttemptResult
from master.async_store import VPSStore


def _envelope(now: float) -> dict[str, Any]:
    """Return the common monitor-agent cache envelope."""

    return {"schema_version": 1, "source": "monitor-agent", "generated_at": now}


def _valid_payloads(now: float) -> dict[str, dict[str, Any]]:
    """Return minimal valid payloads for every cache contract."""

    envelope = _envelope(now)
    return {
        "live_metrics.ndjson": {
            **envelope,
            "ts": now,
            "cpu": 10.0,
            "cpu_60s": 9.0,
            "cpu_60s_window": 60.0,
            "cpu_60s_samples": 61,
            "mem": [100, 50, 50.0, 50],
            "disk": [200, 100, 100, 50.0],
            "swap": [20, 5, 15, 25.0],
            "mem_60s_peak": 50.0,
            "mem_60s_window": 60.0,
            "disk_60s_peak": 50.0,
            "disk_60s_window": 60.0,
            "swap_60s_peak": 25.0,
            "swap_60s_window": 60.0,
            "bots": [],
        },
        "instance_snapshot.json": {
            **envelope,
            "monitors": [],
            "v7": [],
            "cache": {"_version": 2},
            "bot_logs": {},
        },
        "host_meta.json": {
            **envelope,
            "role": "slave",
            "boot": 1,
            "reboot": False,
            "pbgv": "v1",
            "pbgc": "abc",
            "pbgb": "main",
            "pbgpy": "3.12",
            "pb7v": "v7",
            "pb7c": "def",
            "pb7b": "main",
            "pb7py": "3.12",
            "optional_services": {"PBRun": True, "PBCoinData": None},
            "available_logs": ["data/logs/PBGui.log"],
            "systemd_migration": {},
        },
        "service_status.json": {
            **envelope,
            "services": {
                "PBRun": {
                    "status": "running",
                    "pid": 123,
                    "error": None,
                    "was_restarted": False,
                    "expected": True,
                },
            },
        },
        "package_status.json": {**envelope, "upgrades": "2", "reboot": False},
        "collector_status.json": {
            **envelope,
            "hostname": "vps-1",
            "agent_version": "1",
            "loops": {
                name: {"interval": 1.0, "last_ok": now, "last_error": ""}
                for name in monitor_mod.MONITOR_AGENT_LOOP_FILES
            },
        },
    }


def test_post_update_package_helper_writes_fresh_atomic_cache(tmp_path: Path) -> None:
    """Linux update completion can refresh package status without the long-running agent loop."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    apt_get = bin_dir / "apt-get"
    apt_get.write_text("#!/bin/sh\nprintf '0 upgraded, 0 newly installed, 0 to remove and 0 not upgraded.\\n'\n", encoding="utf-8")
    apt_get.chmod(0o700)
    pbgui_dir = tmp_path / "pbgui"
    pbgui_dir.mkdir()
    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}{os.pathsep}{env.get('PATH', '')}"

    result = subprocess.run(
        [sys.executable, "setup/refresh_package_status.py", "--pbgdir", str(pbgui_dir)],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads((pbgui_dir / "data" / "monitor_agent" / "package_status.json").read_text(encoding="utf-8"))
    assert payload["upgrades"] == "0"
    assert payload["source"] == "monitor-agent"
    assert payload["schema_version"] == 1
    assert payload["generated_at"] > 0


def test_monitor_consumes_post_update_package_cache_immediately() -> None:
    """The master can bypass its hourly read throttle for an explicit update completion."""
    monitor = object.__new__(VPSMonitor)
    monitor.pool = SimpleNamespace(get_connection=lambda hostname: object() if hostname == "vps-1" else None)
    monitor.store = SimpleNamespace(host_meta={"vps-1": {"package_status": {"generated_at": 100.0, "upgrades": "5"}}})
    monitor._last_package_status_collect = {"vps-1": 100.0}

    async def collect(hostname: str, *, include_package_status: bool = False) -> None:
        """Replace the SSH collector with a freshly materialized package payload."""
        assert hostname == "vps-1"
        assert include_package_status is True
        monitor.store.host_meta[hostname]["package_status"] = {"generated_at": 200.0, "upgrades": "0"}

    monitor.collect_host_meta_now = collect

    assert asyncio.run(monitor.refresh_package_status("vps-1")) is True
    assert monitor._last_package_status_collect == {}
    assert monitor.store.host_meta["vps-1"]["package_status"]["upgrades"] == "0"


@pytest.mark.parametrize("filename", tuple(monitor_mod.MONITOR_AGENT_FILE_TTLS))
def test_validator_accepts_each_contract_and_additional_fields(filename: str) -> None:
    """Every cache schema accepts its required shape plus safe extensions."""

    now = 1000.0
    payload = _valid_payloads(now)[filename]
    payload["safe_extension"] = {"value": True}

    generated_at, age = monitor_mod._validate_monitor_agent_payload(filename, payload, now=now + 1)

    assert generated_at == now
    assert age == 1.0


def test_host_meta_accepts_complete_optional_pb8_contract_and_rejects_partial_data() -> None:
    """Rolling upgrades accept old metadata, while reported PB8 state stays strict."""
    payload = _valid_payloads(1000.0)["host_meta.json"]
    payload.update({
        "pb8v": "v8.0.0",
        "pb8_config_schema": "v8.0.0",
        "pb8c": "abc",
        "pb8b": "master",
        "pb8py": "3.12",
        "pb8ready": True,
    })

    monitor_mod._validate_monitor_agent_payload("host_meta.json", payload, now=1001.0)
    payload.pop("pb8py")
    with pytest.raises(MonitorAgentPayloadError, match="missing field pb8py"):
        monitor_mod._validate_monitor_agent_payload("host_meta.json", payload, now=1001.0)


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        (lambda payload: payload.update(schema_version=2), "schema_version"),
        (lambda payload: payload.update(source="other"), "source"),
        (lambda payload: payload.update(generated_at=1300.001), "future"),
        (lambda payload: payload.update(generated_at="1000"), "number"),
        (lambda payload: payload.pop("source"), "missing field source"),
    ],
)
def test_validator_rejects_strict_common_contract(mutation, expected: str) -> None:
    """Schema, source, timestamp type, future time, and required fields are strict."""

    payload = _valid_payloads(1000.0)["package_status.json"]
    mutation(payload)

    with pytest.raises(MonitorAgentPayloadError, match=expected):
        monitor_mod._validate_monitor_agent_payload("package_status.json", payload, now=1000.0)


@pytest.mark.parametrize("filename", ("live_metrics.ndjson", "collector_status.json"))
def test_validator_normalizes_shipped_schema_v1_source_compatibility(filename: str) -> None:
    """Shipped schema-v1 live and heartbeat payloads without source remain readable."""

    payload = _valid_payloads(1000.0)[filename]
    payload.pop("source")

    generated_at, age = monitor_mod._validate_monitor_agent_payload(filename, payload, now=1001.0)

    assert generated_at == 1000.0
    assert age == 1.0
    assert payload["source"] == "monitor-agent"


@pytest.mark.parametrize(
    "filename",
    ("instance_snapshot.json", "host_meta.json", "service_status.json", "package_status.json"),
)
def test_validator_keeps_source_strict_for_slow_cache_files(filename: str) -> None:
    """Compatibility does not weaken provenance on newly written slow cache files."""

    payload = _valid_payloads(1000.0)[filename]
    payload.pop("source")

    with pytest.raises(MonitorAgentPayloadError, match="missing field source"):
        monitor_mod._validate_monitor_agent_payload(filename, payload, now=1000.0)


@pytest.mark.parametrize(
    ("filename", "mutate"),
    [
        ("package_status.json", lambda payload, value: payload.update(generated_at=value)),
        ("live_metrics.ndjson", lambda payload, value: payload.update(ts=value)),
        (
            "collector_status.json",
            lambda payload, value: payload["loops"]["host_meta"].update(last_ok=value),
        ),
    ],
)
def test_validator_accepts_three_hundred_second_positive_clock_skew(filename: str, mutate) -> None:
    """Remote timestamps at the positive skew boundary are fresh with zero age."""

    payload = _valid_payloads(1000.0)[filename]
    mutate(payload, 1300.0)

    _generated_at, age = monitor_mod._validate_monitor_agent_payload(filename, payload, now=1000.0)

    assert age == 0.0


@pytest.mark.parametrize(
    ("filename", "mutate", "expected"),
    [
        ("package_status.json", lambda payload, value: payload.update(generated_at=value), "generated_at"),
        ("live_metrics.ndjson", lambda payload, value: payload.update(ts=value), "ts"),
        (
            "collector_status.json",
            lambda payload, value: payload["loops"]["host_meta"].update(last_ok=value),
            "last_ok",
        ),
    ],
)
def test_validator_rejects_clock_skew_beyond_three_hundred_seconds(
    filename: str,
    mutate,
    expected: str,
) -> None:
    """Remote timestamps beyond the bounded positive skew are rejected."""

    payload = _valid_payloads(1000.0)[filename]
    mutate(payload, 1300.001)

    with pytest.raises(MonitorAgentPayloadError, match=expected):
        monitor_mod._validate_monitor_agent_payload(filename, payload, now=1000.0)


@pytest.mark.parametrize(
    ("filename", "mutate"),
    [
        ("package_status.json", lambda payload, value: payload.update(generated_at=value)),
        ("live_metrics.ndjson", lambda payload, value: payload.update(ts=value)),
        (
            "collector_status.json",
            lambda payload, value: payload["loops"]["host_meta"].update(last_ok=value),
        ),
    ],
)
@pytest.mark.parametrize("value", (float("nan"), float("inf"), float("-inf")))
def test_validator_rejects_nonfinite_cache_timestamps(filename: str, mutate, value: float) -> None:
    """Envelope, live, and collector loop timestamps must remain finite."""

    payload = _valid_payloads(1000.0)[filename]
    mutate(payload, value)

    with pytest.raises(MonitorAgentPayloadError, match="invalid number"):
        monitor_mod._validate_monitor_agent_payload(filename, payload, now=1000.0)


@pytest.mark.parametrize(
    ("filename", "field", "value"),
    [
        ("live_metrics.ndjson", "mem", [1, 2, 3]),
        ("instance_snapshot.json", "monitors", ["not-an-object"]),
        ("host_meta.json", "reboot", "false"),
        ("service_status.json", "services", []),
        ("package_status.json", "upgrades", 2),
        ("collector_status.json", "loops", []),
    ],
)
def test_validator_rejects_representative_per_file_types(filename: str, field: str, value: Any) -> None:
    """Each file enforces its own required field types."""

    payload = _valid_payloads(1000.0)[filename]
    payload[field] = value

    with pytest.raises(MonitorAgentPayloadError):
        monitor_mod._validate_monitor_agent_payload(filename, payload, now=1000.0)


def test_agent_writes_canonical_live_path_and_managed_rotation(monkeypatch, tmp_path: Path) -> None:
    """Live NDJSON append and byte rotation use the one canonical cache path and scope."""

    calls: list[tuple[str, Path, str]] = []
    monkeypatch.setattr(monitor_agent, "DATA_DIR", tmp_path / "data" / "monitor_agent")
    monkeypatch.setattr(
        logging_helpers,
        "append_managed_transcript_line",
        lambda path, line, scope: calls.append((line, path, scope)),
    )
    monkeypatch.setattr(
        logging_helpers,
        "rotate_managed_log_before_open",
        lambda path, scope: calls.append(("rotate", path, scope)),
    )

    monitor_agent._append_live_sample({"source": "monitor-agent"})
    monitor_agent._rotate_live_samples()

    expected = tmp_path / "data" / "monitor_agent" / "live_metrics.ndjson"
    assert [(path, scope) for _line, path, scope in calls] == [
        (expected, "monitor_agent_live"),
        (expected, "monitor_agent_live"),
    ]
    assert "NDJSON_RETENTION_SECONDS" not in inspect.getsource(monitor_agent)


def test_agent_json_writers_add_contract_envelopes(monkeypatch, tmp_path: Path) -> None:
    """Slow writers, live construction, and collector health all emit schema/source/time."""

    monkeypatch.setattr(monitor_agent, "DATA_DIR", tmp_path)
    monkeypatch.setattr(monitor_agent, "_embedded_monitor_script", lambda _name: "collector")
    monkeypatch.setattr(monitor_agent, "_script_env", lambda *args, **kwargs: {})
    monkeypatch.setattr(monitor_agent, "_local_credential_capability", lambda: {})
    payloads = iter([
        {"monitors": [], "v7": [], "cache": {"_version": 2}, "bot_logs": {}},
        _valid_payloads(1000.0)["host_meta.json"] | {"schema_version": 0, "source": "old", "generated_at": 0},
        {"upgrades": "1", "reboot": False},
    ])
    monkeypatch.setattr(monitor_agent, "_run_shell_script", lambda *args, **kwargs: next(payloads))

    monitor_agent._run_instance_snapshot()
    monitor_agent._run_host_meta()
    monitor_agent._run_package_status()
    monkeypatch.setattr(monitor_agent, "PBGUI_SERVICES", {
        "PBRun": ("pbgui-pbrun.service", "data/pid/pbrun.pid", "pbrun.py"),
    })
    monkeypatch.setattr(monitor_agent, "_auto_heal_enabled", lambda: False)
    monkeypatch.setattr(monitor_agent, "_service_expected", lambda _name: True)
    monkeypatch.setattr(monitor_agent, "_service_status", lambda *_args: {
        "status": "running", "pid": 1, "error": None, "was_restarted": False,
    })
    monitor_agent._run_service_status()

    for filename in ("instance_snapshot.json", "host_meta.json", "package_status.json", "service_status.json"):
        payload = json.loads((tmp_path / filename).read_text(encoding="utf-8"))
        assert payload["schema_version"] == 1
        assert payload["source"] == "monitor-agent"
        assert payload["generated_at"] > 0
    collector = monitor_agent._collector_status({})
    assert collector["schema_version"] == 1
    assert collector["source"] == "monitor-agent"
    assert '"source": "monitor-agent"' in inspect.getsource(monitor_agent.run)


@pytest.mark.parametrize(
    "payload",
    (None, {}, {"upgrades": "N/A", "reboot": False}, {"upgrades": "two", "reboot": False},
     {"upgrades": 2, "reboot": False}, {"upgrades": "2", "reboot": "false"}),
)
def test_package_writer_rejects_invalid_results_without_replacing_prior_cache(
    monkeypatch,
    tmp_path: Path,
    payload: Any,
) -> None:
    """Missing or ambiguous package results leave the last valid cache untouched."""

    old_payload = {**_envelope(900.0), "upgrades": "7", "reboot": False}
    package_path = tmp_path / "package_status.json"
    package_path.write_text(json.dumps(old_payload), encoding="utf-8")
    monkeypatch.setattr(monitor_agent, "DATA_DIR", tmp_path)
    monkeypatch.setattr(monitor_agent, "_embedded_monitor_script", lambda _name: "collector")
    monkeypatch.setattr(monitor_agent, "_script_env", lambda: {})
    monkeypatch.setattr(monitor_agent, "_run_shell_script", lambda *args, **kwargs: payload)

    with pytest.raises(RuntimeError):
        monitor_agent._run_package_status()

    assert json.loads(package_path.read_text(encoding="utf-8")) == old_payload


def test_package_writer_preserves_prior_cache_when_probe_raises(monkeypatch, tmp_path: Path) -> None:
    """Apt probe exceptions propagate to loop health without touching the prior cache."""

    package_path = tmp_path / "package_status.json"
    package_path.write_text('{"upgrades":"5"}', encoding="utf-8")
    monkeypatch.setattr(monitor_agent, "DATA_DIR", tmp_path)
    monkeypatch.setattr(monitor_agent, "_embedded_monitor_script", lambda _name: "collector")
    monkeypatch.setattr(monitor_agent, "_script_env", lambda: {})

    def fail(*_args, **_kwargs):
        raise RuntimeError("apt failed")

    monkeypatch.setattr(monitor_agent, "_run_shell_script", fail)

    with pytest.raises(RuntimeError, match="apt failed"):
        monitor_agent._run_package_status()

    assert package_path.read_text(encoding="utf-8") == '{"upgrades":"5"}'


def test_package_probe_script_fails_closed_on_apt_and_parse_errors() -> None:
    """The embedded apt probe raises instead of publishing an N/A heartbeat."""

    script = monitor_mod.PACKAGE_STATUS_SCRIPT
    assert "if res.returncode != 0:" in script
    assert "if not match:" in script
    assert "'upgrades': 'N/A'" not in script
    assert "except Exception" not in script


def test_tail_command_prefers_canonical_and_falls_back_only_when_absent() -> None:
    """The master prefers current canonical data while retaining rolling legacy reads."""

    command = monitor_mod._monitor_agent_tail_command("software/pbgui")

    assert "data/monitor_agent/live_metrics.ndjson" in command
    assert "data/logs/monitor-agent/live_metrics.ndjson" in command
    assert " -nt " in command
    assert "if [ ! -f " in command
    assert "tail -n 1 -F" in command


class _AsyncLines:
    """Async iterator over fixed stream lines."""

    def __init__(self, lines: list[str]) -> None:
        self._lines = iter(lines)

    def __aiter__(self):
        return self

    async def __anext__(self) -> str:
        try:
            return next(self._lines)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


class _FakeProcess:
    """Minimal asyncssh process double for metrics stream tests."""

    def __init__(self, lines: list[str]) -> None:
        self.stdout = _AsyncLines(lines)
        self.closed = False

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None


def _noop_history() -> SimpleNamespace:
    """Return a history store double with the methods used during stream cleanup."""

    return SimpleNamespace(record=lambda *args, **kwargs: None, maybe_flush=lambda *args, **kwargs: None)


def test_live_stream_valid_invalid_valid_recovery_keeps_last_known(monkeypatch) -> None:
    """Invalid live lines never reach SystemMetrics and a later valid line clears the error."""

    async def exercise() -> None:
        now = time.time()
        first = _valid_payloads(now)["live_metrics.ndjson"]
        invalid = dict(first, source="untrusted", cpu=99.0)
        recovered = dict(first, cpu=22.0)
        process = _FakeProcess([json.dumps(first), json.dumps(invalid), json.dumps(recovered)])
        monitor = object.__new__(VPSMonitor)
        monitor.pool = SimpleNamespace(
            get_remote_pbgui_dir=lambda _host: "software/pbgui",
            start_process=lambda *_args, **_kwargs: asyncio.sleep(0, result=process),
        )
        monitor.store = VPSStore()
        updates: list[float] = []
        original_update = monitor.store.update_system

        def capture(hostname, metrics) -> None:
            updates.append(metrics.cpu)
            original_update(hostname, metrics)

        monitor.store.update_system = capture
        monitor._stream_generations = {"vps-1": 1}
        monitor._stream_started_at = {"vps-1": now}
        monitor._stream_stale_counts = {}
        monitor._host_metric_history = {}
        monitor._bot_cpu_history = _noop_history()
        monitor._bot_metric_history = {}
        monitor._bot_count_history = {}
        monitor._bot_pnl_history = _noop_history()
        monitor._record_host_metric_history = lambda *_args: None

        await monitor._metrics_stream("vps-1", 1)

        assert updates == [10.0, 22.0]
        assert monitor.store.system["vps-1"].cpu == 22.0
        live = monitor.store.streams["vps-1"]["monitor_agent"]["files"]["live_metrics.ndjson"]
        assert live["state"] == "ok"
        assert live["error"] is None
        assert process.closed is True

    asyncio.run(exercise())


def test_diagnostics_recompute_ttls_and_precedence(monkeypatch) -> None:
    """Effective ages honor every file TTL and error beats missing/stale/unknown/ok."""

    monitor = object.__new__(VPSMonitor)
    monitor.store = VPSStore()
    generated = {
        "live_metrics.ndjson": 84.0,
        "instance_snapshot.json": 20.0,
        "host_meta.json": 69.0,
        "service_status.json": 0.1,
        "package_status.json": 1.0,
    }
    for filename, generated_at in generated.items():
        monitor._update_monitor_agent_file_status("vps-1", filename, {
            "state": "ok",
            "error": None,
            "generated_at": generated_at,
            "checked_at": generated_at,
        })
    monitor._update_monitor_agent_file_status("vps-1", "collector_status.json", {
        "state": "missing",
        "error": "missing",
        "checked_at": 100.0,
    })
    files = monitor.store.streams["vps-1"]["monitor_agent"]["files"]
    assert files["live_metrics.ndjson"]["state"] == "stale"
    assert files["instance_snapshot.json"]["state"] == "ok"
    assert files["host_meta.json"]["state"] == "stale"
    assert files["service_status.json"]["state"] == "ok"
    assert files["package_status.json"]["state"] == "ok"
    assert monitor.store.streams["vps-1"]["monitor_agent"]["state"] == "missing"

    payload = _valid_payloads(100.0)["collector_status.json"]
    payload["loops"]["host_meta"]["last_error"] = "secret traceback text"
    monkeypatch.setattr(monitor_mod.time, "time", lambda: 100.0)
    monitor._apply_collector_status("vps-1", payload)
    agent = monitor.store.streams["vps-1"]["monitor_agent"]
    assert agent["state"] == "error"
    assert agent["files"]["host_meta.json"]["state"] == "error"
    assert agent["files"]["instance_snapshot.json"]["state"] == "ok"
    assert "secret" not in json.dumps(agent)


def test_collector_error_recovery_and_metadata(monkeypatch) -> None:
    """A recovered loop clears only its collector error and retains collector provenance."""

    monitor = object.__new__(VPSMonitor)
    monitor.store = VPSStore()
    monitor._update_monitor_agent_file_status("vps-1", "host_meta.json", {
        "state": "ok", "error": None, "generated_at": 99.0, "checked_at": 100.0,
    })
    failed = _valid_payloads(100.0)["collector_status.json"]
    failed["loops"]["host_meta"]["last_error"] = "RuntimeError"
    monkeypatch.setattr(monitor_mod.time, "time", lambda: 100.0)
    monitor._apply_collector_status("vps-1", failed)
    recovered = _valid_payloads(101.0)["collector_status.json"]
    monkeypatch.setattr(monitor_mod.time, "time", lambda: 101.0)

    monitor._apply_collector_status("vps-1", recovered)

    agent = monitor.store.streams["vps-1"]["monitor_agent"]
    assert agent["files"]["host_meta.json"]["state"] == "ok"
    assert agent["files"]["host_meta.json"]["error"] is None
    assert agent["collector"] == {
        "source": "monitor-agent",
        "hostname": "vps-1",
        "agent_version": "1",
        "generated_at": 101.0,
        "age": 0.0,
        "checked_at": 101.0,
    }
    assert set(agent["files"]) == set(monitor_mod.MONITOR_AGENT_FILE_TTLS)


@pytest.mark.parametrize("heartbeat_state", ("missing", "stale"))
def test_unavailable_heartbeat_demotes_prior_loop_errors_and_fresh_status_restores(
    monkeypatch,
    heartbeat_state: str,
) -> None:
    """Current heartbeat availability supersedes expired per-loop error diagnostics."""

    monitor = object.__new__(VPSMonitor)
    monitor.store = VPSStore()
    monitor._update_monitor_agent_file_status("vps-1", "host_meta.json", {
        "state": "ok", "error": None, "generated_at": 99.0, "checked_at": 100.0,
    })
    failed = _valid_payloads(100.0)["collector_status.json"]
    failed["loops"]["host_meta"]["last_error"] = "RuntimeError"
    monkeypatch.setattr(monitor_mod.time, "time", lambda: 100.0)
    monitor._apply_collector_status("vps-1", failed)

    monitor._update_monitor_agent_file_status("vps-1", "collector_status.json", {
        "state": heartbeat_state,
        "error": f"heartbeat {heartbeat_state}",
        "generated_at": 69.0 if heartbeat_state == "stale" else 0.0,
        "checked_at": 101.0,
        "source": "monitor-agent",
    })

    unavailable = monitor.store.streams["vps-1"]["monitor_agent"]
    assert unavailable["state"] == heartbeat_state
    assert unavailable["files"]["host_meta.json"]["state"] == "ok"
    assert "collector_error" not in unavailable["files"]["host_meta.json"]
    assert unavailable["loops"] == {}
    assert "collector" not in unavailable

    fresh = _valid_payloads(102.0)["collector_status.json"]
    monkeypatch.setattr(monitor_mod.time, "time", lambda: 102.0)
    monitor._update_monitor_agent_file_status("vps-1", "collector_status.json", {
        "state": "ok", "error": None, "generated_at": 102.0, "checked_at": 102.0,
    })
    monitor._apply_collector_status("vps-1", fresh)

    restored = monitor.store.streams["vps-1"]["monitor_agent"]
    assert set(restored["loops"]) == set(monitor_mod.MONITOR_AGENT_LOOP_FILES)
    assert restored["collector"]["generated_at"] == 102.0


class _CachePool:
    """Return cache payloads by filename and capture all SSH commands."""

    def __init__(self, payloads: dict[str, dict[str, Any] | None]) -> None:
        self.payloads = payloads
        self.commands: list[str] = []

    def get_remote_pbgui_dir(self, _hostname: str) -> str:
        return "software/pbgui"

    async def run(self, _hostname: str, command: str, **_kwargs):
        self.commands.append(command)
        filename = next(name for name in monitor_mod.MONITOR_AGENT_FILE_TTLS if name in command)
        payload = self.payloads.get(filename)
        if payload is None:
            return SimpleNamespace(exit_status=1, stdout="", stderr="private material")
        return SimpleNamespace(exit_status=0, stdout=json.dumps(payload), stderr="")


def test_host_meta_cache_miss_has_no_fallback_and_package_is_nested() -> None:
    """A miss retains host metadata, sends no probe, and stores package provenance nested."""

    async def exercise() -> None:
        now = time.time()
        pool = _CachePool({
            "collector_status.json": _valid_payloads(now)["collector_status.json"],
            "host_meta.json": None,
            "package_status.json": _valid_payloads(now)["package_status.json"],
        })
        monitor = object.__new__(VPSMonitor)
        monitor.pool = pool
        monitor.store = VPSStore()
        monitor.store.host_meta["vps-1"] = {"role": "last-known", "source": "monitor-agent"}
        monitor._last_host_meta_collect = {}
        monitor._last_package_status_collect = {}
        monitor._host_meta_collecting = set()
        monitor._debug_logging = False
        monitor._cache_host_snapshot = lambda _hostname: None

        await monitor._collect_host_meta("vps-1", include_package_status=True, force=True)

        meta = monitor.store.host_meta["vps-1"]
        assert meta["role"] == "last-known"
        assert meta["source"] == "monitor-agent"
        assert meta["package_status"]["source"] == "monitor-agent"
        assert meta["upgrades"] == "2"
        assert len(pool.commands) == 3
        assert all(command.startswith("cat ") for command in pool.commands)
        assert "_collect_host_meta_direct" not in inspect.getsource(monitor_mod)
        assert "direct-ssh" not in inspect.getsource(monitor_mod)

    asyncio.run(exercise())


def test_master_preserves_fresh_package_payload_provenance_without_count_backfill() -> None:
    """A legacy N/A package payload is stored intact instead of borrowing an old count."""

    async def exercise() -> None:
        now = time.time()
        package = _valid_payloads(now)["package_status.json"]
        package["upgrades"] = "N/A"
        pool = _CachePool({
            "collector_status.json": _valid_payloads(now)["collector_status.json"],
            "host_meta.json": _valid_payloads(now)["host_meta.json"],
            "package_status.json": package,
        })
        monitor = object.__new__(VPSMonitor)
        monitor.pool = pool
        monitor.store = VPSStore()
        monitor.store.host_meta["vps-1"] = {
            "package_status": {**_envelope(now - 100.0), "upgrades": "8", "reboot": False},
            "upgrades": "8",
        }
        monitor._last_host_meta_collect = {}
        monitor._last_package_status_collect = {}
        monitor._host_meta_collecting = set()
        monitor._debug_logging = False
        monitor._cache_host_snapshot = lambda _hostname: None

        await monitor._collect_host_meta("vps-1", include_package_status=True, force=True)

        stored = monitor.store.host_meta["vps-1"]
        assert stored["package_status"] == package
        assert stored["package_status"]["generated_at"] == now
        assert stored["upgrades"] == "N/A"

    asyncio.run(exercise())


def test_every_host_meta_cycle_reads_collector_status() -> None:
    """Collector heartbeat is consumed even when slower cache reads are not due."""

    async def exercise() -> None:
        monitor = object.__new__(VPSMonitor)
        monitor._host_meta_collecting = set()
        monitor._last_host_meta_collect = {"vps-1": 100.0}
        monitor._last_package_status_collect = {"vps-1": 100.0}
        read_files: list[str] = []

        async def read(_hostname: str, filename: str, **_kwargs):
            read_files.append(filename)
            return _valid_payloads(100.0)[filename]

        monitor._read_monitor_agent_json = read
        monitor._apply_collector_status = lambda *_args: None
        original_time = monitor_mod.time.time
        monitor_mod.time.time = lambda: 105.0
        try:
            await monitor._collect_host_meta("vps-1")
        finally:
            monitor_mod.time.time = original_time

        assert read_files == ["collector_status.json"]

    asyncio.run(exercise())


def test_collector_heartbeat_stale_after_thirty_seconds() -> None:
    """A collector heartbeat older than 30 seconds is rejected and diagnosed stale."""

    async def exercise() -> None:
        now = time.time()
        pool = _CachePool({"collector_status.json": _valid_payloads(now - 31.0)["collector_status.json"]})
        monitor = object.__new__(VPSMonitor)
        monitor.pool = pool
        monitor.store = VPSStore()

        payload = await monitor._read_monitor_agent_json(
            "vps-1", "collector_status.json", stale_after=30.0
        )

        assert payload is None
        collector = monitor.store.streams["vps-1"]["monitor_agent"]["files"]["collector_status.json"]
        assert collector["state"] == "stale"
        assert collector["age"] >= 31.0

    asyncio.run(exercise())


def test_host_meta_tasks_are_owned_and_cancelled_before_disconnect() -> None:
    """Shutdown closes admission and drains host tasks before closing SSH."""

    async def exercise() -> None:
        monitor = object.__new__(VPSMonitor)
        monitor._running = True
        monitor.loop = asyncio.get_running_loop()
        monitor._tasks = []
        monitor._stream_tasks = {}
        monitor._host_meta_tasks = set()
        monitor._config_retry_task = None
        monitor._config_changed = asyncio.Event()
        monitor._ini_watcher = SimpleNamespace(stop=lambda: None, unbind_asyncio=lambda: None)
        monitor._host_metric_history = {}
        monitor._bot_cpu_history = _noop_history()
        monitor._bot_metric_history = {}
        monitor._bot_count_history = {}
        monitor._bot_pnl_history = _noop_history()
        order: list[str] = []
        late_admissions: list[asyncio.Task | None] = []

        async def late() -> None:
            order.append("late-ran")

        async def pending() -> None:
            try:
                await asyncio.Event().wait()
            finally:
                order.append("host-meta-cancelled")
                late_admissions.append(monitor._create_host_meta_task(
                    late(), hostname="vps-1", name="host-meta-late-vps-1",
                ))

        class Pool:
            """Record disconnect ordering."""

            async def disconnect_all(self) -> None:
                order.append("disconnect")

        monitor.pool = Pool()
        task = monitor._create_host_meta_task(pending(), hostname="vps-1", name="host-meta-manual-vps-1")
        await asyncio.sleep(0)
        assert task is not None
        assert task in monitor._host_meta_tasks

        await monitor.stop()
        await monitor.stop()

        assert order == ["host-meta-cancelled", "disconnect"]
        assert late_admissions == [None]
        assert monitor._host_meta_tasks == set()
        assert task.cancelled()

    asyncio.run(exercise())


def test_host_meta_task_creation_paths_use_dedicated_owner() -> None:
    """Startup, reconnect, enable, and refresh use the dedicated host-meta task set."""

    source = inspect.getsource(VPSMonitor)
    for task_name in (
        "host-meta-startup-",
        "host-meta-reconnect-",
        "host-meta-enable-",
        "host-meta-refresh-",
        "host-meta-cycle-",
    ):
        marker = source.index(task_name)
        preceding = source[max(0, marker - 250):marker]
        assert "_create_host_meta_task(" in preceding


def test_public_manual_refresh_owns_child_and_isolates_caller_cancellation() -> None:
    """Cancelling a manual caller never registers or cancels its monitor-owned child."""

    async def exercise() -> None:
        monitor = object.__new__(VPSMonitor)
        monitor._running = True
        monitor.pool = SimpleNamespace(get_connection=lambda _host: object())
        monitor._host_meta_tasks = set()
        entered = asyncio.Event()
        release = asyncio.Event()

        async def collect(*_args, **_kwargs) -> None:
            entered.set()
            await release.wait()

        monitor._collect_host_meta = collect
        caller = asyncio.create_task(monitor.collect_host_meta_now("vps-1"), name="external-manual")
        await entered.wait()
        assert caller not in monitor._host_meta_tasks
        assert len(monitor._host_meta_tasks) == 1
        child = next(iter(monitor._host_meta_tasks))
        assert child.get_name() == "host-meta-manual-vps-1"
        assert getattr(child, "_pbgui_host_meta_host") == "vps-1"

        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller
        assert not child.cancelled()
        assert not child.done()

        release.set()
        await child
        await asyncio.sleep(0)
        assert monitor._host_meta_tasks == set()

    asyncio.run(exercise())


def test_host_disable_closes_generation_and_awaits_tasks_before_pool_removal() -> None:
    """Per-host admission closes before cancellation and pool removal."""

    async def exercise() -> None:
        monitor = object.__new__(VPSMonitor)
        monitor._running = True
        monitor._host_meta_tasks = set()
        order: list[str] = []

        async def pending() -> None:
            try:
                await asyncio.Event().wait()
            finally:
                order.append("cancelled")

        task = monitor._create_host_meta_task(pending(), hostname="vps-1", name="host-meta-vps-1")
        await asyncio.sleep(0)
        assert task is not None
        initial_generation = getattr(task, "_pbgui_host_meta_host_generation")

        monitor._set_host_meta_host_admission("vps-1", enabled=False)
        rejected = monitor._create_host_meta_task(
            pending(), hostname="vps-1", name="host-meta-rejected-vps-1",
        )
        await monitor._cancel_host_meta_tasks("vps-1")
        order.append("pool-remove")

        assert rejected is None
        assert order == ["cancelled", "pool-remove"]
        assert monitor._host_meta_host_generations["vps-1"] == initial_generation + 1
        assert monitor._host_meta_tasks == set()

    asyncio.run(exercise())


class _RefreshRaceConnection:
    """Connection double whose close state can be asserted."""

    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        """Record closure without performing I/O."""

        self.closed = True


class _RefreshRacePool:
    """Event-gated pool double exposing the connection installed by each attempt."""

    def __init__(self, attempts: int = 1, *, reuse_current_attempts: set[int] | None = None) -> None:
        self._hosts = ["vps-1"]
        self.entry = SimpleNamespace(conn=None, status=monitor_mod.ConnectionStatus.DISCONNECTED)
        self.entered = [asyncio.Event() for _ in range(attempts)]
        self.release = [asyncio.Event() for _ in range(attempts)]
        self.installed = [asyncio.Event() for _ in range(attempts)]
        self.return_release = [asyncio.Event() for _ in range(attempts)]
        for event in self.return_release:
            event.set()
        self.connections = [_RefreshRaceConnection() for _ in range(attempts)]
        self.reuse_current_attempts = set(reuse_current_attempts or set())
        self._next_attempt = 0
        self._connect_lock = asyncio.Lock()

    def load_vps_configs(self) -> list[str]:
        """Return the configured host without changing test state."""

        return list(self._hosts)

    def hostnames(self) -> list[str]:
        """Return current pool membership."""

        return list(self._hosts)

    def get_connection(self, hostname: str):
        """Return the shared entry while the host remains admitted to the pool."""

        return self.entry if hostname in self._hosts else None

    async def connect_with_result(self, _hostname: str) -> ConnectionAttemptResult:
        """Install or reuse a connection and return its stable identity."""

        attempt = self._next_attempt
        self._next_attempt += 1
        self.entered[attempt].set()
        await self.release[attempt].wait()
        async with self._connect_lock:
            current = self.entry.conn
            reused = attempt in self.reuse_current_attempts and current is not None
            connection = current if reused else self.connections[attempt]
            self.entry.conn = connection
            self.entry.status = monitor_mod.ConnectionStatus.CONNECTED
            self.installed[attempt].set()
        await self.return_release[attempt].wait()
        return ConnectionAttemptResult(True, connection, not reused)

    async def close_created_connection(
        self,
        _hostname: str,
        connection: _RefreshRaceConnection,
    ) -> None:
        """Conditionally clear an exact handle while preserving newer state."""

        async with self._connect_lock:
            if self.entry.conn is connection:
                self.entry.conn = None
                self.entry.status = monitor_mod.ConnectionStatus.DISCONNECTED
            connection.close()

    async def disconnect(self, _hostname: str) -> None:
        """Close the currently installed connection, if any."""

        connection = self.entry.conn
        self.entry.conn = None
        self.entry.status = monitor_mod.ConnectionStatus.DISCONNECTED
        if connection is not None:
            connection.close()

    async def disconnect_all(self) -> None:
        """Disconnect the only host."""

        await self.disconnect("vps-1")

    def remove_host(self, hostname: str) -> None:
        """Remove one host from pool membership."""

        self._hosts = [host for host in self._hosts if host != hostname]


def _refresh_race_monitor(pool: _RefreshRacePool) -> tuple[VPSMonitor, list[str], list[str]]:
    """Build a minimal running monitor for deterministic refresh lifecycle tests."""

    monitor = object.__new__(VPSMonitor)
    monitor.pool = pool
    monitor._running = True
    monitor.loop = asyncio.get_running_loop()
    monitor._enabled_hosts = {"vps-1"}
    monitor._tasks = []
    monitor._stream_tasks = {}
    monitor._stream_generations = {}
    monitor._host_meta_tasks = set()
    monitor._host_meta_task_admission_open = True
    monitor._host_meta_lifecycle_generation = 1
    monitor._host_meta_host_generations = {}
    monitor._host_meta_blocked_hosts = set()
    monitor._last_host_meta_collect = {"vps-1": 1.0}
    monitor._last_package_status_collect = {"vps-1": 1.0}
    monitor._config_retry_task = None
    monitor._config_changed = asyncio.Event()
    monitor._ini_watcher = SimpleNamespace(stop=lambda: None, unbind_asyncio=lambda: None)
    monitor._host_metric_history = {}
    monitor._bot_cpu_history = _noop_history()
    monitor._bot_metric_history = {}
    monitor._bot_count_history = {}
    monitor._bot_pnl_history = _noop_history()
    streams_started: list[str] = []
    metadata_started: list[str] = []
    monitor._start_metrics_stream = lambda hostname: streams_started.append(hostname)

    async def collect(hostname: str, **_kwargs) -> None:
        metadata_started.append(hostname)

    monitor.collect_host_meta_now = collect
    return monitor, streams_started, metadata_started


def test_refresh_enabled_host_stop_during_connect_cannot_resurrect_tasks() -> None:
    """A connect completing after stop is closed and starts no monitor work."""

    async def exercise() -> None:
        pool = _RefreshRacePool()
        monitor, streams_started, metadata_started = _refresh_race_monitor(pool)
        refresh = asyncio.create_task(monitor.refresh_enabled_host("vps-1"))
        await pool.entered[0].wait()

        await monitor.stop()
        pool.release[0].set()

        assert await refresh is False
        assert pool.connections[0].closed is True
        assert pool.entry.conn is None
        assert streams_started == []
        assert metadata_started == []
        assert monitor._host_meta_task_admission_open is False
        assert monitor._host_meta_tasks == set()

    asyncio.run(exercise())


def test_refresh_enabled_host_disable_during_connect_cannot_resurrect_tasks() -> None:
    """A disabled host rejects and closes a late successful connection."""

    async def exercise() -> None:
        pool = _RefreshRacePool()
        monitor, streams_started, metadata_started = _refresh_race_monitor(pool)
        refresh = asyncio.create_task(monitor.refresh_enabled_host("vps-1"))
        await pool.entered[0].wait()

        monitor._enabled_hosts.clear()
        monitor._set_host_meta_host_admission("vps-1", enabled=False)
        pool.release[0].set()

        assert await refresh is False
        assert pool.connections[0].closed is True
        assert pool.entry.conn is None
        assert streams_started == []
        assert metadata_started == []
        assert "vps-1" in monitor._host_meta_blocked_hosts
        assert monitor._host_meta_tasks == set()

    asyncio.run(exercise())


def test_newer_refresh_generation_supersedes_old_without_disconnect() -> None:
    """Only the newest refresh may own the live connection and launch tasks."""

    async def exercise() -> None:
        pool = _RefreshRacePool(attempts=2)
        monitor, streams_started, metadata_started = _refresh_race_monitor(pool)
        old_refresh = asyncio.create_task(monitor.refresh_enabled_host("vps-1"))
        await pool.entered[0].wait()
        new_refresh = asyncio.create_task(monitor.refresh_enabled_host("vps-1"))
        await pool.entered[1].wait()

        pool.release[0].set()
        assert await old_refresh is False
        assert pool.connections[0].closed is False
        assert pool.entry.conn is pool.connections[0]

        pool.release[1].set()
        assert await new_refresh is True
        await asyncio.sleep(0)

        assert pool.connections[1].closed is False
        assert pool.entry.conn is pool.connections[1]
        assert streams_started == ["vps-1"]
        assert metadata_started == ["vps-1"]
        assert monitor._host_meta_host_generations["vps-1"] == 2

    asyncio.run(exercise())


def test_async_ssh_pool_connect_remains_bool_delegate() -> None:
    """The original connect API still returns a plain success boolean."""

    async def exercise() -> None:
        pool = AsyncSSHPool()
        connection = _RefreshRaceConnection()

        async def connect_with_result(_hostname: str) -> ConnectionAttemptResult:
            return ConnectionAttemptResult(True, connection, False)

        pool.connect_with_result = connect_with_result

        assert await pool.connect("vps-1") is True

    asyncio.run(exercise())


def test_stale_refresh_does_not_close_reused_connection() -> None:
    """A stale attempt never closes a connection it merely observed."""

    async def exercise() -> None:
        pool = _RefreshRacePool()
        existing = pool.connections[0]
        pool.entry.conn = existing
        pool.entry.status = monitor_mod.ConnectionStatus.CONNECTED

        async def reuse_connection(_hostname: str) -> ConnectionAttemptResult:
            pool.entered[0].set()
            await pool.release[0].wait()
            return ConnectionAttemptResult(True, existing, False)

        pool.connect_with_result = reuse_connection
        monitor, streams_started, metadata_started = _refresh_race_monitor(pool)
        refresh = asyncio.create_task(monitor.refresh_enabled_host("vps-1"))
        await pool.entered[0].wait()

        monitor._host_meta_host_generations["vps-1"] += 1
        pool.release[0].set()

        assert await refresh is False
        assert existing.closed is False
        assert pool.entry.conn is existing
        assert streams_started == []
        assert metadata_started == []

    asyncio.run(exercise())


def test_stale_refresh_preserves_distinct_handle_after_newer_install() -> None:
    """Generation-only staleness leaves both A and newer current handle B open."""

    async def exercise() -> None:
        pool = _RefreshRacePool(attempts=2)
        pool.return_release[0].clear()
        monitor, streams_started, metadata_started = _refresh_race_monitor(pool)
        old_refresh = asyncio.create_task(monitor.refresh_enabled_host("vps-1"))
        await pool.entered[0].wait()
        new_refresh = asyncio.create_task(monitor.refresh_enabled_host("vps-1"))
        await pool.entered[1].wait()

        pool.release[0].set()
        await pool.installed[0].wait()
        pool.release[1].set()
        assert await new_refresh is True
        assert pool.entry.conn is pool.connections[1]

        pool.return_release[0].set()
        assert await old_refresh is False
        await asyncio.sleep(0)

        assert pool.connections[0].closed is False
        assert pool.connections[1].closed is False
        assert pool.entry.conn is pool.connections[1]
        assert pool.entry.status == monitor_mod.ConnectionStatus.CONNECTED
        assert streams_started == ["vps-1"]
        assert metadata_started == ["vps-1"]

    asyncio.run(exercise())


def test_stale_refresh_preserves_handle_reused_by_newer_refresh() -> None:
    """B may adopt A's handle before A observes generation-only staleness."""

    async def exercise() -> None:
        pool = _RefreshRacePool(attempts=2, reuse_current_attempts={1})
        pool.return_release[0].clear()
        monitor, streams_started, metadata_started = _refresh_race_monitor(pool)
        old_refresh = asyncio.create_task(monitor.refresh_enabled_host("vps-1"))
        await pool.entered[0].wait()
        new_refresh = asyncio.create_task(monitor.refresh_enabled_host("vps-1"))
        await pool.entered[1].wait()

        pool.release[0].set()
        await pool.installed[0].wait()
        connection = pool.connections[0]
        pool.release[1].set()
        assert await new_refresh is True
        assert pool.entry.conn is connection

        pool.return_release[0].set()
        assert await old_refresh is False
        await asyncio.sleep(0)

        assert connection.closed is False
        assert pool.entry.conn is connection
        assert pool.entry.status == monitor_mod.ConnectionStatus.CONNECTED
        assert streams_started == ["vps-1"]
        assert metadata_started == ["vps-1"]

    asyncio.run(exercise())


def test_store_host_meta_merge_preserves_dimensions() -> None:
    """Store merges package provenance without erasing existing host metadata."""

    store = VPSStore()
    store.update_host_meta("vps-1", {"role": "slave", "source": "monitor-agent"})
    package = _valid_payloads(100.0)["package_status.json"]
    store.update_host_meta("vps-1", {"package_status": package, "upgrades": "2"})

    assert store.host_meta["vps-1"] == {
        "role": "slave",
        "source": "monitor-agent",
        "package_status": package,
        "upgrades": "2",
    }
