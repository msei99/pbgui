"""Contract tests for agent-backed VPS Manager package status."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import vps_manager_service as service_mod
from vps_manager_service import (
    MONITOR_AGENT_REQUIRED_FILES,
    VPSManagerService,
    _normalize_monitor_agent,
    _normalize_package_status,
)


ROOT = Path(__file__).resolve().parents[1]


def _package_payload(generated_at: float, *, upgrades: object = "4", reboot: object = True) -> dict:
    """Build one valid package-status agent payload."""

    return {
        "schema_version": 1,
        "source": "monitor-agent",
        "generated_at": generated_at,
        "upgrades": upgrades,
        "reboot": reboot,
    }


def _remote_agent(generated_at: float, checked_at: float) -> dict:
    """Build remote monitor-agent metadata for all required files."""

    return {
        "source": "monitor-agent",
        "state": "ok",
        "checked_at": checked_at,
        "collector": {
            "source": "monitor-agent",
            "generated_at": generated_at,
            "checked_at": checked_at,
            "age": checked_at - generated_at,
        },
        "files": {
            filename: {
                "state": "ok",
                "generated_at": generated_at,
                "checked_at": checked_at,
                "age": checked_at - generated_at,
            }
            for filename in MONITOR_AGENT_REQUIRED_FILES
        },
        "loops": {
            "package_status": {
                "interval": 3600,
                "last_ok": generated_at,
                "last_error": "",
            }
        },
    }


def _local_payloads(now: float) -> dict[str, dict]:
    """Build strict payloads for every local monitor-agent cache."""

    envelope = {"schema_version": 1, "source": "monitor-agent", "generated_at": now}
    return {
        "live_metrics.ndjson": {
            **envelope, "ts": now, "cpu": 1.0, "cpu_60s": 1.0,
            "cpu_60s_window": 60.0, "cpu_60s_samples": 60,
            "mem": [1, 1, 0.0, 1], "disk": [1, 0, 1, 0.0],
            "swap": [0, 0, 0, 0.0], "mem_60s_peak": 0.0,
            "mem_60s_window": 60.0, "disk_60s_peak": 0.0,
            "disk_60s_window": 60.0, "swap_60s_peak": 0.0,
            "swap_60s_window": 60.0, "bots": [],
        },
        "instance_snapshot.json": {**envelope, "monitors": [], "v7": [], "cache": {"_version": 2}, "bot_logs": {}},
        "host_meta.json": {
            **envelope, "role": "master", "boot": 1, "reboot": False,
            "pbgv": "v1", "pbgc": "a", "pbgb": "main", "pbgpy": "3.12",
            "pb7v": "v7", "pb7c": "b", "pb7b": "master", "pb7py": "3.12",
            "optional_services": {}, "available_logs": [], "systemd_migration": {},
        },
        "service_status.json": {**envelope, "services": {}},
        "package_status.json": {**envelope, "upgrades": "2", "reboot": True},
        "collector_status.json": {
            **envelope, "hostname": "master", "agent_version": "1",
            "loops": {
                name: {"interval": 60.0, "last_ok": now, "last_error": ""}
                for name in service_mod.MONITOR_AGENT_LOOP_FILES
            },
        },
    }


def _write_local_payloads(root: Path, now: float) -> Path:
    """Write all strict local cache payloads below a temporary PBGui root."""

    data_dir = root / "data" / "monitor_agent"
    data_dir.mkdir(parents=True)
    for filename, payload in _local_payloads(now).items():
        text = json.dumps(payload) + ("\n" if filename.endswith(".ndjson") else "")
        (data_dir / filename).write_text(text, encoding="utf-8")
    return data_dir


def test_vps_manager_contains_no_direct_package_probe() -> None:
    """Package status must never execute a local or remote simulation probe."""

    core_source = (ROOT / "vps_manager_core.py").read_text(encoding="utf-8")
    service_source = (ROOT / "vps_manager_service.py").read_text(encoding="utf-8")
    combined = core_source + service_source

    assert "fetch_package_status" not in combined
    assert "apt-get" not in combined
    assert "dist-upgrade" not in combined
    assert "_vps_package_status_cache" not in combined
    assert "_refresh_local_package_status" not in combined


def test_package_status_normalizes_and_age_progresses() -> None:
    """Validated cache values keep their provenance while age advances."""

    payload = _package_payload(9_900, upgrades="7", reboot=False)
    file_status = {"state": "ok", "generated_at": 9_900, "checked_at": 10_000, "age": 100}

    first = _normalize_package_status(payload, file_status=file_status, now=10_000)
    later = _normalize_package_status(payload, file_status=file_status, now=10_025)

    assert first == {
        "source": "agent_cache",
        "state": "ok",
        "available": True,
        "upgrades": 7,
        "reboot_required": False,
        "generated_at": 9_900.0,
        "checked_at": 10_000.0,
        "age": 100.0,
        "error": None,
    }
    assert later["age"] == 125.0


def test_stale_values_remain_last_known_but_na_is_never_zero() -> None:
    """Stale numeric values remain available while N/A stays unavailable."""

    stale = _normalize_package_status(_package_payload(1_000, upgrades=3, reboot=True), now=10_000)
    unavailable = _normalize_package_status(_package_payload(9_990, upgrades="N/A", reboot=False), now=10_000)

    assert stale["state"] == "stale"
    assert stale["available"] is True
    assert stale["upgrades"] == 3
    assert stale["reboot_required"] is True
    assert unavailable["state"] == "error"
    assert unavailable["available"] is False
    assert unavailable["upgrades"] == "N/A"
    assert unavailable["reboot_required"] is None


@pytest.mark.parametrize(
    ("payload", "error_fragment"),
    [
        ({"schema_version": 1, "source": "direct-ssh", "generated_at": 9_990, "upgrades": 1, "reboot": False}, "invalid source"),
        ({"schema_version": 99, "source": "monitor-agent", "generated_at": 9_990, "upgrades": 1, "reboot": False}, "unsupported schema"),
        ({"schema_version": 1, "source": "monitor-agent", "generated_at": 0, "upgrades": 1, "reboot": False}, "invalid generated_at"),
    ],
)
def test_package_status_rejects_invalid_provenance(payload: dict, error_fragment: str) -> None:
    """Reject package data that does not satisfy the agent contract."""

    result = _normalize_package_status(payload, now=10_000)

    assert result["state"] == "error"
    assert result["available"] is False
    assert error_fragment in str(result["error"])


def test_remote_contract_is_password_independent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remote normalization consumes only monitor state, never VPS credentials."""

    now = 20_000.0
    monkeypatch.setattr(service_mod.time, "time", lambda: now)
    service = object.__new__(VPSManagerService)
    host_state = {
        "meta": {"package_status": _package_payload(now - 20, upgrades=5, reboot=False)},
        "stream": {"monitor_agent": _remote_agent(now - 20, now - 5)},
    }

    package_status, monitor_agent = service._get_remote_agent_contract(host_state)

    assert package_status["source"] == "agent_cache"
    assert package_status["state"] == "ok"
    assert package_status["upgrades"] == 5
    assert package_status["age"] == 20.0
    assert monitor_agent["source"] == "agent_cache"
    assert set(monitor_agent["files"]) == set(MONITOR_AGENT_REQUIRED_FILES)


def test_remote_agent_effective_ages_progress(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remote file ages continue advancing between monitor snapshots."""

    raw = _remote_agent(29_990, 30_000)
    first = _normalize_monitor_agent(raw, now=30_000)
    later = _normalize_monitor_agent(raw, now=30_012)

    assert first["files"]["package_status.json"]["effective_age"] == 10.0
    assert later["files"]["package_status.json"]["effective_age"] == 22.0
    assert later["files"]["live_metrics.ndjson"]["state"] == "stale"


def test_generated_at_wins_over_inconsistent_reported_age() -> None:
    """Generated timestamps prevent age plus elapsed-time double counting."""

    raw = _remote_agent(39_990, 39_999)
    raw["files"]["package_status.json"]["age"] = 500

    result = _normalize_monitor_agent(raw, now=40_000)

    assert result["files"]["package_status.json"]["effective_age"] == 10.0


def test_remote_unknown_placeholder_normalizes_to_missing() -> None:
    """An uncollected producer placeholder is missing data, not an agent error."""

    result = _normalize_monitor_agent({
        "source": "monitor-agent",
        "state": "unknown",
        "checked_at": 30_000,
        "files": {
            filename: {"state": "unknown", "checked_at": 30_000}
            for filename in MONITOR_AGENT_REQUIRED_FILES
        },
    }, now=30_000)

    assert result["state"] == "missing"
    assert all(item["state"] == "missing" for item in result["files"].values())


def test_local_contract_reads_json_only_and_reports_collector_loops(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The local master reads package and collector caches without subprocesses."""

    data_dir = tmp_path / "data" / "monitor_agent"
    data_dir.mkdir(parents=True)
    now = 40_000.0
    (data_dir / "package_status.json").write_text(
        json.dumps(_package_payload(now - 10, upgrades="2", reboot=True)),
        encoding="utf-8",
    )
    (data_dir / "collector_status.json").write_text(
        json.dumps({
            "schema_version": 1,
            "source": "monitor-agent",
            "generated_at": now - 5,
            "hostname": "master",
            "agent_version": "1",
            "loops": {
                "live_metrics": {"interval": 1, "last_ok": now - 2, "last_error": ""},
                "instances": {"interval": 30, "last_ok": now - 3, "last_error": ""},
                "host_meta": {"interval": 30, "last_ok": now - 4, "last_error": ""},
                "services": {"interval": 60, "last_ok": now - 5, "last_error": "collector <failed>"},
                "package_status": {"interval": 3600, "last_ok": now - 10, "last_error": ""},
            },
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(service_mod, "PBGDIR", tmp_path)
    clock = [now]
    monkeypatch.setattr(service_mod.time, "time", lambda: clock[0])
    monkeypatch.setattr(service_mod.subprocess, "run", lambda *args, **kwargs: pytest.fail("subprocess must not run"))
    service = object.__new__(VPSManagerService)

    package_status, monitor_agent = service._get_local_agent_contract()
    clock[0] += 7
    later_package, later_agent = service._get_local_agent_contract()

    assert package_status["upgrades"] == 2
    assert package_status["reboot_required"] is True
    assert monitor_agent["source"] == "agent_cache"
    assert monitor_agent["state"] == "error"
    assert monitor_agent["loops"]["services"]["last_error"] == "collector <failed>"
    assert set(monitor_agent["files"]) == set(MONITOR_AGENT_REQUIRED_FILES)
    assert later_package["age"] == package_status["age"] + 7
    assert later_agent["files"]["collector_status.json"]["effective_age"] == monitor_agent["files"]["collector_status.json"]["effective_age"] + 7


@pytest.mark.parametrize("failure", ["deleted", "directory", "corrupt", "symlink"])
def test_local_contract_requires_every_regular_valid_cache_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure: str,
) -> None:
    """Missing, invalid, and symlinked local caches can never report healthy."""

    now = 50_000.0
    data_dir = _write_local_payloads(tmp_path, now - 1)
    target = data_dir / "service_status.json"
    if failure == "deleted":
        target.unlink()
    elif failure == "directory":
        target.unlink()
        target.mkdir()
    elif failure == "corrupt":
        target.write_text("{not-json", encoding="utf-8")
    else:
        outside = tmp_path / "outside.json"
        outside.write_text(json.dumps(_local_payloads(now - 1)["service_status.json"]), encoding="utf-8")
        target.unlink()
        target.symlink_to(outside)
    monkeypatch.setattr(service_mod, "PBGDIR", tmp_path)
    monkeypatch.setattr(service_mod.time, "time", lambda: now)
    monkeypatch.setattr(service_mod.subprocess, "run", lambda *args, **kwargs: pytest.fail("subprocess must not run"))
    service = object.__new__(VPSManagerService)

    _package_status, monitor_agent = service._get_local_agent_contract()

    assert monitor_agent["state"] != "ok"
    assert monitor_agent["files"]["service_status.json"]["state"] in {"missing", "error"}
    assert set(monitor_agent["files"]) == set(MONITOR_AGENT_REQUIRED_FILES)


def test_local_contract_reports_ok_only_when_all_six_files_validate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """All six independent file checks plus collector loops are required for OK."""

    now = 60_000.0
    _write_local_payloads(tmp_path, now - 1)
    monkeypatch.setattr(service_mod, "PBGDIR", tmp_path)
    monkeypatch.setattr(service_mod.time, "time", lambda: now)
    service = object.__new__(VPSManagerService)

    package_status, monitor_agent = service._get_local_agent_contract()

    assert package_status["state"] == "ok"
    assert monitor_agent["state"] == "ok"
    assert all(item["state"] == "ok" for item in monitor_agent["files"].values())
    assert all(item["state"] == "ok" for item in monitor_agent["loops"].values())


def test_legacy_response_fields_keep_boolean_and_readiness_semantics() -> None:
    """Tri-state package truth must not change established outward field contracts."""

    unknown_package = _normalize_package_status(None, now=70_000)
    service = object.__new__(VPSManagerService)
    service.vpsmanager = SimpleNamespace(update_status="", command_text="", last_update="")
    service._build_master_overview_row = lambda: {
        "online": True, "updates": "N/A", "reboot_required": False,
        "package_status": unknown_package, "monitor_agent": {},
        "pbgui_github": "", "pb7_github": "",
    }
    service._local_credential_capability_metadata = lambda: {}

    master_status = service._build_master_status(False)

    assert master_status["update_ready"] is True
    assert master_status["summary_row"]["reboot_required"] is False
    assert isinstance(master_status["summary_row"]["reboot_required"], bool)
    assert master_status["package_status"]["reboot_required"] is None

    vps = SimpleNamespace(
        hostname="vps-1", user_pw="available", init_status="", setup_status="", update_status="",
        command_text="", last_update="", last_setup="", last_init="", user="user", remote_pbgui_dir="/home/user/pbgui",
        is_vps_in_hosts=lambda: True,
    )
    service._build_vps_overview_row = lambda *_args, **_kwargs: {
        "updates": "N/A", "reboot_required": False, "package_status": unknown_package,
        "monitor_agent": {}, "pbgui_github": "", "pb7_github": "",
    }
    service._cluster_node_status = lambda _host: {}
    service._build_remote_pbgui_github_status = lambda _state: ""
    service._build_remote_pb7_github_status = lambda _state: ""
    service._host_online = lambda _state: True
    service._host_telemetry_fresh = lambda _state: True
    service._host_telemetry_age = lambda _state: 0.0
    service._host_meta = lambda _state: {}
    service._credential_capability_metadata = lambda *_args: {}
    service._build_remote_server_metrics = lambda *_args: {}
    service._get_vps_systemd_migration_status = lambda *_args, **_kwargs: {}
    service._vps_ssh_ok_cache = {}

    vps_status = service._build_vps_status(vps, {}, quick=True)

    assert vps_status["update_ready"] is True
    assert vps_status["summary_row"]["reboot_required"] is False
    assert isinstance(vps_status["summary_row"]["reboot_required"], bool)
    assert vps_status["package_status"]["reboot_required"] is None


def test_unknown_credential_capability_remains_none_in_playbook_vars() -> None:
    """Deployment serialization must not coerce unknown credential state to false."""

    service = object.__new__(VPSManagerService)
    service._credential_capability_metadata = lambda *_args: {
        "credential_protocol_version": 2,
        "credential_active": None,
        "cmc_catalog_generation": None,
        "cmc_materialized_generation": None,
        "cmc_active_key_count": None,
    }
    service._host_meta = lambda _state: {"role": "vps"}

    result = service._credential_playbook_vars("vps-1", {})

    assert "credential_active" in result
    assert result["credential_active"] is None
    assert '"credential_active": null' in json.dumps(result, sort_keys=True)
