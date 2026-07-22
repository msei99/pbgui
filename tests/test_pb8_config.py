"""Unit tests for the isolated PB8 configuration client."""

from __future__ import annotations

import json
from pathlib import Path

import pb8_config
import pb8_config_helper
import pytest
from master_update_lock import MasterUpdateBusyError


def _reset_cache(monkeypatch) -> None:
    """Keep cache tests isolated from process-global PB8 client state."""
    monkeypatch.setattr(pb8_config, "_template_cache", None)
    monkeypatch.setattr(pb8_config, "_result_metrics_cache", None)
    monkeypatch.setattr(pb8_config, "_optimize_metadata_cache", None)
    pb8_config._config_cache.clear()


def test_prepare_pb8_config_delegates_to_isolated_helper(monkeypatch) -> None:
    """In-memory configs must be prepared by the PB8 helper, not imported locally."""
    calls = []

    def fake_call(operation: str, **payload) -> dict:
        calls.append((operation, payload))
        return {"config": {"config_version": "v8.0.0"}}

    monkeypatch.setattr(pb8_config, "_call_helper", fake_call)

    result = pb8_config.prepare_pb8_config({"backtest": {}}, base_config_path="/tmp/backtest.json")

    assert result == {"config_version": "v8.0.0"}
    assert calls == [
        (
            "prepare",
            {
                "config": {"backtest": {}},
                "base_config_path": "/tmp/backtest.json",
            },
        )
    ]


def test_result_metrics_use_bounded_helper_cache(monkeypatch) -> None:
    """Installed PB8 metric names should be normalized, copied, and cached."""
    _reset_cache(monkeypatch)
    calls = []

    def fake_call(operation: str, **payload) -> dict:
        calls.append((operation, payload))
        return {"metrics": ["sharpe_ratio", "adg", "adg"]}

    monkeypatch.setattr(pb8_config, "_call_helper", fake_call)

    first = pb8_config.get_pb8_result_metrics()
    first.append("mutated")

    assert pb8_config.get_pb8_result_metrics() == ["adg", "sharpe_ratio"]
    assert calls == [("result_metrics", {})]


def test_runtime_fingerprint_change_invalidates_optimize_metadata_cache(monkeypatch) -> None:
    """A PB8 update must invalidate metadata before the 30-second TTL expires."""
    _reset_cache(monkeypatch)
    fingerprint = ["commit-a"]
    calls = []

    def fake_call(operation: str, **_payload) -> dict:
        calls.append(operation)
        return {"template": {"runtime": fingerprint[0]}, "strategies": []}

    monkeypatch.setattr(pb8_config, "_runtime_fingerprint", lambda *_args: tuple(fingerprint))
    monkeypatch.setattr(pb8_config, "_call_helper", fake_call)

    assert pb8_config.get_pb8_optimize_metadata()["template"]["runtime"] == "commit-a"
    assert pb8_config.get_pb8_optimize_metadata()["template"]["runtime"] == "commit-a"
    fingerprint[0] = "commit-b"
    assert pb8_config.get_pb8_optimize_metadata()["template"]["runtime"] == "commit-b"
    assert calls == ["optimize_metadata", "optimize_metadata"]


def test_runtime_fingerprint_change_invalidates_loaded_config_cache(tmp_path, monkeypatch) -> None:
    """Canonical configs cached by file signature must also belong to the current PB8 runtime."""
    _reset_cache(monkeypatch)
    source = tmp_path / "backtest.json"
    source.write_text("{}", encoding="utf-8")
    fingerprint = ["commit-a"]
    calls = []

    def fake_call(operation: str, **_payload) -> dict:
        calls.append(operation)
        return {"config": {"runtime": fingerprint[0]}}

    monkeypatch.setattr(pb8_config, "_runtime_fingerprint", lambda *_args: tuple(fingerprint))
    monkeypatch.setattr(pb8_config, "_call_helper", fake_call)

    assert pb8_config.load_pb8_config(source) == {"runtime": "commit-a"}
    assert pb8_config.load_pb8_config(source) == {"runtime": "commit-a"}
    fingerprint[0] = "commit-b"
    assert pb8_config.load_pb8_config(source) == {"runtime": "commit-b"}
    assert calls == ["load", "load"]


def test_call_helper_uses_pb8_venv_cwd_and_releases_update_lock(monkeypatch) -> None:
    """PB8 helper subprocesses hold and release the master runtime lease."""
    released = []
    captured = {}

    class Lease:
        def release(self) -> None:
            released.append(True)

    class Proc:
        returncode = 0
        stdout = '{"ok":true,"result":{"version":"v8"}}'
        stderr = ""

    monkeypatch.setattr(pb8_config, "acquire_master_runtime_lock", lambda _root: Lease())
    monkeypatch.setattr(
        pb8_config,
        "pb8_runtime_status",
        lambda: {"ready": True, "pb8dir": "/runtime/pb8", "pb8venv": "/runtime/venv/bin/python"},
    )

    def fake_run(command, **kwargs):
        captured.update(command=command, kwargs=kwargs)
        return Proc()

    monkeypatch.setattr(pb8_config.subprocess, "run", fake_run)

    assert pb8_config._call_helper("status") == {"version": "v8"}
    assert captured["command"][0] == "/runtime/venv/bin/python"
    assert captured["kwargs"]["cwd"] == "/runtime/pb8"
    assert released == [True]


def test_call_helper_transforms_update_lock_busy_without_subprocess(monkeypatch) -> None:
    """A PB8 update remains distinguishable and never starts a helper process."""
    busy = MasterUpdateBusyError("update active")
    monkeypatch.setattr(
        pb8_config,
        "acquire_master_runtime_lock",
        lambda _root: (_ for _ in ()).throw(busy),
    )
    monkeypatch.setattr(
        pb8_config.subprocess,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("subprocess started")),
    )

    with pytest.raises(pb8_config.PB8RuntimeBusyError, match="Retry") as error:
        pb8_config._call_helper("status")

    assert error.value.retryable is True
    assert error.value.status_code == 503
    assert error.value.__cause__ is busy


def test_save_pb8_config_writes_prepared_config_atomically(tmp_path, monkeypatch) -> None:
    """Saving must persist the canonical helper output rather than the request payload."""
    destination = tmp_path / "demo" / "backtest.json"
    prepared = {"config_version": "v8.0.0", "backtest": {"base_dir": "backtests/pbgui/demo"}}
    monkeypatch.setattr(pb8_config, "prepare_pb8_config", lambda *_args, **_kwargs: prepared)

    result = pb8_config.save_pb8_config({"legacy": True}, destination)

    assert result == prepared
    assert json.loads(destination.read_text(encoding="utf-8")) == prepared
    assert not list(destination.parent.glob(f".{destination.name}.*"))


def test_template_and_file_loads_use_bounded_signature_cache(tmp_path, monkeypatch) -> None:
    """Repeated reads avoid helper startup while file changes invalidate cached configs."""
    _reset_cache(monkeypatch)
    source = tmp_path / "backtest.json"
    source.write_text('{"version": 1}', encoding="utf-8")
    calls = []

    def fake_call(operation: str, **payload) -> dict:
        calls.append((operation, payload))
        if operation == "default":
            return {"config": {"template": True}}
        return {"config": json.loads(source.read_text(encoding="utf-8"))}

    monkeypatch.setattr(pb8_config, "_call_helper", fake_call)

    first_template = pb8_config.get_pb8_template_config()
    first_template["mutated"] = True
    assert pb8_config.get_pb8_template_config() == {"template": True}
    first_load = pb8_config.load_pb8_config(source)
    first_load["mutated"] = True
    assert pb8_config.load_pb8_config(source) == {"version": 1}
    source.write_text('{"version": 200}', encoding="utf-8")
    assert pb8_config.load_pb8_config(source) == {"version": 200}
    assert [operation for operation, _payload in calls] == ["default", "load", "load"]


def test_save_prepared_pb8_config_skips_second_helper_and_warms_load_cache(tmp_path, monkeypatch) -> None:
    """Already prepared API payloads should write atomically and load without another helper process."""
    _reset_cache(monkeypatch)
    destination = tmp_path / "demo" / "backtest.json"
    prepared = {"config_version": "v8.0.0", "backtest": {"base_dir": "backtests/pbgui/demo"}}
    monkeypatch.setattr(pb8_config, "_call_helper", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("helper called")))

    result = pb8_config.save_prepared_pb8_config(prepared, destination)

    assert result == prepared
    assert pb8_config.load_pb8_config(destination) == prepared
    assert json.loads(destination.read_text(encoding="utf-8")) == prepared


def test_migrate_pb7_config_passes_distinct_absolute_paths(tmp_path, monkeypatch) -> None:
    """The client should provide explicit source and output paths to PB8's migration helper."""
    source = tmp_path / "v7" / "backtest.json"
    output = tmp_path / "v8" / "backtest.json"
    captured = {}

    def fake_call(operation: str, **payload) -> dict:
        captured.update({"operation": operation, **payload})
        return {"report": {"output_written": True}, "config": {}}

    monkeypatch.setattr(pb8_config, "_call_helper", fake_call)

    result = pb8_config.migrate_pb7_config(source, output)

    assert result["report"]["output_written"] is True
    assert captured == {
        "operation": "migrate_v7",
        "source_path": str(source.resolve()),
        "output_path": str(output.resolve()),
        "allow_manual_review_output": False,
    }


def test_helper_prepare_preserves_pbgui_metadata_outside_pb8_payload() -> None:
    """PBGui-owned metadata must survive while remaining invisible to PB8 canonicalization."""
    received = {}

    def prepare(config, **_kwargs):
        received.update(config)
        return {"config_version": "v8.0.0", "backtest": {}}

    modules = {"prepare_config": prepare, "sanitize": lambda value: value}
    metadata = {"market_cap": 25, "future": {"keep": True}}

    result = pb8_config_helper._prepare(modules, {"backtest": {}, "pbgui": metadata})

    assert "pbgui" not in received
    assert result["pbgui"] == metadata


def test_helper_load_restores_nested_pbgui_metadata(tmp_path, monkeypatch) -> None:
    """Loading a stored PB8 config must merge its opaque PBGui envelope back in."""
    source = tmp_path / "backtest.json"
    metadata = {"tags": ["defi"], "future": {"enabled": True}}
    source.write_text(json.dumps({"backtest": {}, "pbgui": metadata}), encoding="utf-8")
    modules = {
        "load_prepared_config": lambda *_args, **_kwargs: {"config_version": "v8.0.0", "backtest": {}},
        "sanitize": lambda value: value,
    }
    monkeypatch.setattr(pb8_config_helper, "_load_pb8_modules", lambda _path: modules)

    result = pb8_config_helper.handle({"operation": "load", "pb8_dir": str(tmp_path), "config_path": str(source)})

    assert result["config"]["pbgui"] == metadata


def test_optimize_metadata_builds_nonempty_bounds_and_bot_defaults_for_every_strategy() -> None:
    """The strategy selector must receive real per-strategy controls, not empty placeholders."""
    strategies = ("trailing_martingale", "ema_anchor", "trailing_grid_v7")
    all_bounds = {
        side: {
            "risk": {"n_positions": [1, 10, 1]},
            "strategy": {kind: {"entry": {"value": [index, index + 1, 0.1]}} for index, kind in enumerate(strategies)},
        }
        for side in ("long", "short")
    }
    template = {
        "bot": {side: {"strategy": {strategies[0]: {"entry": {"value": 0}}}} for side in ("long", "short")},
        "live": {"strategy_kind": strategies[0]},
        "optimize": {"bounds": {side: {"risk": {"n_positions": [1, 10, 1]}, "strategy": {strategies[0]: {"entry": {"value": [0, 1, 0.1]}}}} for side in ("long", "short")}},
    }
    modules = {
        "get_template_config": lambda: template,
        "prepare_config": lambda config, **_kwargs: config,
        "sanitize": lambda value: value,
        "get_supported_strategy_kinds": lambda: strategies,
        "get_strategy_spec": lambda kind: {"kind": kind},
        "get_all_strategy_defaults": lambda: {side: {kind: {"entry": {"value": index}} for index, kind in enumerate(strategies)} for side in ("long", "short")},
        "get_optimize_bounds_defaults": lambda: all_bounds,
        "result_metrics": [],
        "default_objective_goals": {},
        "backends": ["pymoo"],
        "pymoo_algorithms": ["nsga2"],
        "pymoo_ref_dir_methods": ["das_dennis"],
        "objective_goals": ["min", "max"],
        "limit_statistics": ["mean"],
        "optimizer_overrides": [],
        "fixed_runtime_overrides": {},
    }

    metadata = pb8_config_helper._optimize_metadata(modules)

    assert set(metadata["active_bounds"]) == set(strategies)
    for kind in strategies:
        for side in ("long", "short"):
            assert metadata["active_bounds"][kind][side]["strategy"] == {
                kind: all_bounds[side]["strategy"][kind]
            }
            assert metadata["strategy_defaults"][side][kind]
