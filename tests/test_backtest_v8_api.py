"""Regression tests for PB8 backtest persistence and process isolation."""

from __future__ import annotations

import gzip
import json
import os
import threading
import time
import uuid
from pathlib import Path

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from api import backtest_v8
from master_update_lock import acquire_master_update_lock


def _patch_roots(tmp_path: Path, monkeypatch) -> tuple[Path, Path, Path, Path]:
    """Redirect all PB8 backtest state to an isolated temporary tree."""
    configs = tmp_path / "data" / "bt_v8"
    v7_configs = tmp_path / "data" / "bt_v7"
    queue = tmp_path / "data" / "bt_v8_queue"
    logs = tmp_path / "data" / "logs" / "backtests_v8"
    monkeypatch.setattr(backtest_v8, "_configs_dir", lambda: configs)
    monkeypatch.setattr(backtest_v8, "_v7_configs_dir", lambda: v7_configs)
    monkeypatch.setattr(backtest_v8, "_queue_dir", lambda: queue)
    monkeypatch.setattr(backtest_v8, "_log_dir", lambda: logs)
    monkeypatch.setattr(backtest_v8, "PBGDIR", str(tmp_path))
    return configs, v7_configs, queue, logs


def test_optimize_and_queue_drafts_round_trip_isolated_copies() -> None:
    """PB8 cross-page drafts must validate payloads and not expose mutable store values."""
    backtest_v8._opt_draft_store.clear()
    backtest_v8._queue_draft_store.clear()
    config = {"config_version": "v8.0.0", "bot": {"long": {"risk": {"n_positions": 3}}}}

    optimize_id = backtest_v8.create_optimize_draft({"config": config}, session=None)["draft_id"]
    config["bot"]["long"]["risk"]["n_positions"] = 99
    optimize_payload = backtest_v8.get_optimize_draft(optimize_id, session=None)
    assert optimize_payload["config"]["bot"]["long"]["risk"]["n_positions"] == 3

    queue_id = backtest_v8.create_queue_draft(
        {"items": [{"name": "candidate", "config": optimize_payload["config"]}]},
        session=None,
    )["draft_id"]
    queue_payload = backtest_v8.get_queue_draft(queue_id, session=None)
    assert queue_payload["items"] == [{"name": "candidate", "config": optimize_payload["config"]}]

    with pytest.raises(HTTPException) as error:
        backtest_v8.create_queue_draft({"items": []}, session=None)
    assert error.value.status_code == 422


def test_concurrent_pb8_draft_creation_stays_bounded() -> None:
    """Parallel FastAPI worker threads must not corrupt or overfill draft stores."""
    backtest_v8._opt_draft_store.clear()
    backtest_v8._queue_draft_store.clear()
    errors: list[Exception] = []

    def create_drafts(worker: int) -> None:
        try:
            for index in range(40):
                backtest_v8.create_optimize_draft({"config": {"worker": worker, "index": index}}, session=None)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=create_drafts, args=(worker,)) for worker in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert errors == []
    assert all(not thread.is_alive() for thread in threads)
    assert len(backtest_v8._opt_draft_store) == backtest_v8._MAX_DRAFTS


def test_ohlcv_preload_logs_and_transforms_validation_failure(monkeypatch) -> None:
    """PB8 preload validation failures must be logged and exposed as HTTP 422."""
    messages = []
    monkeypatch.setattr(
        backtest_v8,
        "start_pb8_ohlcv_preload_job",
        lambda _config: (_ for _ in ()).throw(ValueError("source not ready")),
    )
    monkeypatch.setattr(
        backtest_v8,
        "_log",
        lambda service, message, **kwargs: messages.append((service, message, kwargs)),
    )

    with pytest.raises(HTTPException) as error:
        backtest_v8.start_ohlcv_preload({"config": {}}, None)

    assert error.value.status_code == 422
    assert error.value.detail == "source not ready"
    assert any("OHLCV preload failed" in message for _service, message, _kwargs in messages)


def test_migrate_v7_keeps_source_and_persists_report(tmp_path, monkeypatch) -> None:
    """Successful conversion must never modify the saved PB7 source config."""
    configs, v7_configs, _queue, _logs = _patch_roots(tmp_path, monkeypatch)
    source = v7_configs / "demo" / "backtest.json"
    source.parent.mkdir(parents=True)
    source_payload = {
        "config_version": "v7",
        "backtest": {"starting_balance": 1000},
        "live": {"base_config_path": "/tmp/stale.json"},
        "pbgui": {"market_cap": 25},
    }
    source.write_text(json.dumps(source_payload), encoding="utf-8")
    report = {"output_written": True, "status": "ok", "manual_review_fields": []}
    migrated = {"config_version": "v8.0.0", "backtest": {}}

    def fake_migrate(source_path, output_path, **_kwargs):
        assert Path(source_path) != source
        assert json.loads(Path(source_path).read_text(encoding="utf-8")) == {
            "config_version": "v7",
            "backtest": {"starting_balance": 1000},
            "live": {},
        }
        Path(output_path).write_text(json.dumps(migrated), encoding="utf-8")
        return {"report": report, "config": migrated}

    def fake_save(config, path):
        Path(path).write_text(json.dumps(config), encoding="utf-8")
        return config

    monkeypatch.setattr(backtest_v8, "migrate_pb7_config", fake_migrate)
    monkeypatch.setattr(backtest_v8, "save_prepared_pb8_config", fake_save)
    monkeypatch.setattr(backtest_v8, "_log", lambda *_args, **_kwargs: None)

    response = backtest_v8.migrate_v7(
        {"source_name": "demo", "target_name": "demo_v8"},
        session=None,
    )

    assert response["name"] == "demo_v8"
    assert json.loads(source.read_text(encoding="utf-8")) == source_payload
    target = configs / "demo_v8"
    saved_report = json.loads((target / "migration_report.json").read_text(encoding="utf-8"))
    assert saved_report["status"] == "ok"
    assert saved_report["pbgui_source_adjustments"] == ["pbgui", "live.base_config_path"]
    assert json.loads((target / "backtest.json").read_text(encoding="utf-8"))["backtest"]["base_dir"] == "backtests/pbgui/demo_v8"


@pytest.mark.parametrize(
    ("source_type", "filename"),
    [("run_config", "config.json"), ("backtest_result", "config.json")],
)
def test_migrate_v7_accepts_managed_run_and_result_sources(
    tmp_path, monkeypatch, source_type: str, filename: str
) -> None:
    """Run and result conversions must use only their managed source roots."""
    configs, _v7_configs, _queue, _logs = _patch_roots(tmp_path, monkeypatch)
    run_root = tmp_path / "data" / "run_v7"
    result_root = tmp_path / "pb7" / "backtests" / "pbgui"
    monkeypatch.setattr(backtest_v8, "_run_v7_dir", lambda: run_root)
    monkeypatch.setattr(backtest_v8, "_v7_results_dir", lambda: result_root)
    source_dir = run_root / "demo" if source_type == "run_config" else result_root / "demo" / "bybit" / "run-1"
    source_dir.mkdir(parents=True)
    (source_dir / filename).write_text(json.dumps({"backtest": {}}), encoding="utf-8")
    if source_type == "backtest_result":
        (source_dir / "analysis.json").write_text("{}", encoding="utf-8")
    migrated = {"config_version": "v8.0.0", "backtest": {}}

    def fake_migrate(_source_path, output_path, **_kwargs):
        Path(output_path).write_text(json.dumps(migrated), encoding="utf-8")
        return {"report": {"output_written": True, "manual_review_fields": []}, "config": migrated}

    monkeypatch.setattr(backtest_v8, "migrate_pb7_config", fake_migrate)
    monkeypatch.setattr(backtest_v8, "save_prepared_pb8_config", lambda config, _path: config)
    monkeypatch.setattr(backtest_v8, "_log", lambda *_args, **_kwargs: None)
    body = {"source_type": source_type, "source_name": "demo", "target_name": f"{source_type}_v8"}
    if source_type == "backtest_result":
        body["source_path"] = str(source_dir)

    response = backtest_v8.migrate_v7(body, session=None)

    assert response["name"] == f"{source_type}_v8"
    assert (configs / f"{source_type}_v8").is_dir()


def test_migrate_v7_result_uses_effective_fees_recorded_in_fills(tmp_path, monkeypatch) -> None:
    """Result conversion must replace normalized fee defaults with historical fill rates."""
    configs, _v7_configs, _queue, _logs = _patch_roots(tmp_path, monkeypatch)
    pb7_root = tmp_path / "pb7"
    result_root = pb7_root / "backtests" / "pbgui"
    result_dir = result_root / "demo" / "bybit" / "run-1"
    result_dir.mkdir(parents=True)
    source_payload = {
        "config_version": "v7.12.0",
        "backtest": {"maker_fee_override": 0.0004, "taker_fee_override": None},
    }
    (result_dir / "config.json").write_text(json.dumps(source_payload), encoding="utf-8")
    (result_dir / "analysis.json").write_text("{}", encoding="utf-8")
    settings_path = pb7_root / "caches" / "market_specific_settings.json"
    settings_path.parent.mkdir(parents=True)
    settings_path.write_text(
        json.dumps({"HYPE": {"linear": True, "c_mult": 1.0}}),
        encoding="utf-8",
    )
    (result_dir / "dataset.json").write_text(
        json.dumps({"market_specific_settings_file": str(settings_path)}),
        encoding="utf-8",
    )
    (result_dir / "fills.csv").write_text(
        "coin,fee_paid,qty,price,liquidity\n"
        "HYPE,-0.02,1,100,maker\n"
        "HYPE,-0.04,2,100,maker\n"
        "HYPE,-0.055,1,100,taker\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(backtest_v8, "pb7dir", lambda: str(pb7_root))
    monkeypatch.setattr(backtest_v8, "_v7_results_dir", lambda: result_root)
    captured = {}
    migrated = {"config_version": "v8.0.0", "backtest": {}}

    def fake_migrate(source_path, output_path, **_kwargs):
        captured.update(json.loads(Path(source_path).read_text(encoding="utf-8")))
        Path(output_path).write_text(json.dumps(migrated), encoding="utf-8")
        return {
            "report": {"output_written": True, "status": "ok", "manual_review_fields": []},
            "config": migrated,
        }

    monkeypatch.setattr(backtest_v8, "migrate_pb7_config", fake_migrate)
    monkeypatch.setattr(backtest_v8, "save_prepared_pb8_config", lambda config, _path: config)
    monkeypatch.setattr(backtest_v8, "_log", lambda *_args, **_kwargs: None)

    response = backtest_v8.migrate_v7(
        {
            "source_type": "backtest_result",
            "source_name": "demo",
            "source_path": str(result_dir),
            "target_name": "demo_v8",
        },
        session=None,
    )

    assert captured["backtest"]["maker_fee_override"] == pytest.approx(0.0002)
    assert captured["backtest"]["taker_fee_override"] == pytest.approx(0.00055)
    assert json.loads((result_dir / "config.json").read_text(encoding="utf-8")) == source_payload
    adjustments = response["report"]["pbgui_result_fee_adjustments"]
    assert adjustments == [
        {
            "field": "backtest.maker_fee_override",
            "result_config_value": 0.0004,
            "effective_value": pytest.approx(0.0002),
            "evidence": "fills.csv",
        },
        {
            "field": "backtest.taker_fee_override",
            "result_config_value": None,
            "effective_value": pytest.approx(0.00055),
            "evidence": "fills.csv",
        },
    ]
    saved_report = json.loads((configs / "demo_v8" / "migration_report.json").read_text(encoding="utf-8"))
    assert saved_report["pbgui_result_fee_adjustments"] == response["report"]["pbgui_result_fee_adjustments"]
    assert (configs / "demo_v8").is_dir()


def test_migrate_v7_rejects_result_path_outside_pb7_root(tmp_path, monkeypatch) -> None:
    """Browser-provided result paths must not select arbitrary config files."""
    _patch_roots(tmp_path, monkeypatch)
    result_root = tmp_path / "pb7" / "backtests" / "pbgui"
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "config.json").write_text("{}", encoding="utf-8")
    (outside / "analysis.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(backtest_v8, "_v7_results_dir", lambda: result_root)

    with pytest.raises(HTTPException) as error:
        backtest_v8.migrate_v7(
            {
                "source_type": "backtest_result",
                "source_name": "demo",
                "source_path": str(outside),
                "target_name": "demo_v8",
            },
            session=None,
        )

    assert error.value.status_code == 400


def test_migrate_v7_rejects_existing_target_with_409(tmp_path, monkeypatch) -> None:
    """Conversion must not overwrite an existing V8 config."""
    configs, v7_configs, _queue, _logs = _patch_roots(tmp_path, monkeypatch)
    source = v7_configs / "demo" / "backtest.json"
    source.parent.mkdir(parents=True)
    source.write_text("{}", encoding="utf-8")
    (configs / "demo_v8").mkdir(parents=True)

    with pytest.raises(HTTPException) as error:
        backtest_v8.migrate_v7(
            {"source_name": "demo", "target_name": "demo_v8"},
            session=None,
        )

    assert error.value.status_code == 409


def test_failed_migration_publishes_no_v8_config(tmp_path, monkeypatch) -> None:
    """Manual-review migration output must not leave a usable target or staging directory."""
    configs, v7_configs, _queue, _logs = _patch_roots(tmp_path, monkeypatch)
    source = v7_configs / "demo" / "backtest.json"
    source.parent.mkdir(parents=True)
    source.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        backtest_v8,
        "migrate_pb7_config",
        lambda *_args, **_kwargs: {
            "report": {"output_written": False, "manual_review_fields": ["bot.long.example"]}
        },
    )

    with pytest.raises(HTTPException) as error:
        backtest_v8.migrate_v7(
            {"source_name": "demo", "target_name": "demo_v8"},
            session=None,
        )

    assert error.value.status_code == 422
    assert not (configs / "demo_v8").exists()
    assert not list(configs.glob(".migrate-*"))


def test_manual_review_output_is_not_published_as_runnable_config(tmp_path, monkeypatch) -> None:
    """Best-effort PB8 output remains unpublished while review fields are unresolved."""
    configs, v7_configs, _queue, _logs = _patch_roots(tmp_path, monkeypatch)
    source = v7_configs / "demo" / "backtest.json"
    source.parent.mkdir(parents=True)
    source.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        backtest_v8,
        "migrate_pb7_config",
        lambda *_args, **_kwargs: {
            "report": {"output_written": True, "manual_review_fields": ["bot.long.example"]},
            "config": {"config_version": "v8.0.0", "backtest": {}},
        },
    )

    with pytest.raises(HTTPException) as error:
        backtest_v8.migrate_v7(
            {
                "source_name": "demo",
                "target_name": "demo_v8",
                "allow_manual_review_output": True,
            },
            session=None,
        )

    assert error.value.status_code == 422
    assert not (configs / "demo_v8").exists()


def test_create_only_save_rejects_concurrent_existing_name(tmp_path, monkeypatch) -> None:
    """Create semantics must return 409 instead of replacing a config from another tab."""
    configs, _v7_configs, _queue, _logs = _patch_roots(tmp_path, monkeypatch)
    target = configs / "demo" / "backtest.json"
    target.parent.mkdir(parents=True)
    target.write_text("{}", encoding="utf-8")

    with pytest.raises(HTTPException) as error:
        backtest_v8.save_config(
            "demo",
            {"config_version": "v8.0.0", "backtest": {}},
            create_only=True,
            session=None,
        )

    assert error.value.status_code == 409
    assert target.read_text(encoding="utf-8") == "{}"


def test_bundle_save_publishes_new_sparse_override_with_config(tmp_path, monkeypatch) -> None:
    """A new PB8 config and its sparse override must become visible as one bundle."""
    configs, _v7_configs, _queue, _logs = _patch_roots(tmp_path, monkeypatch)
    monkeypatch.setattr(backtest_v8, "prepare_pb8_config", lambda config, **_kwargs: config)
    monkeypatch.setattr(backtest_v8, "cache_prepared_pb8_config", lambda *_args: None)
    config = {
        "config_version": "v8.0.0",
        "backtest": {},
        "coin_overrides": {"HYPE": {"override_config_path": "HYPE.json"}},
    }
    sparse = {"bot": {"long": {"risk": {"n_positions": 3}}}}

    result = backtest_v8.save_config(
        "demo",
        {"config": config, "override_configs": {"HYPE.json": sparse}},
        create_only=True,
        session=None,
    )

    assert result["ok"] is True
    assert json.loads((configs / "demo" / "HYPE.json").read_text(encoding="utf-8")) == sparse
    assert json.loads((configs / "demo" / "backtest.json").read_text(encoding="utf-8"))["coin_overrides"] == config["coin_overrides"]


def test_confirmed_fresh_replacement_does_not_inherit_target_overrides(tmp_path, monkeypatch) -> None:
    """Replacing from an import/new editor must reject missing files instead of borrowing target contents."""
    configs, _v7_configs, _queue, _logs = _patch_roots(tmp_path, monkeypatch)
    target = configs / "demo"
    target.mkdir(parents=True)
    (target / "backtest.json").write_text("{}", encoding="utf-8")
    (target / "HYPE.json").write_text('{"old": true}', encoding="utf-8")
    monkeypatch.setattr(backtest_v8, "prepare_pb8_config", lambda config, **_kwargs: config)
    config = {"backtest": {}, "coin_overrides": {"HYPE": {"override_config_path": "HYPE.json"}}}

    with pytest.raises(HTTPException) as error:
        backtest_v8.save_config(
            "demo",
            {"config": config, "override_configs": {}},
            inherit_existing_overrides=False,
            session=None,
        )

    assert error.value.status_code == 422
    assert (target / "HYPE.json").read_text(encoding="utf-8") == '{"old": true}'


def test_config_name_cannot_mimic_transaction_artifact(tmp_path, monkeypatch) -> None:
    """Hidden config names must not be deletable by transaction recovery parsing."""
    _patch_roots(tmp_path, monkeypatch)

    with pytest.raises(HTTPException) as error:
        backtest_v8.save_config(f".demo.stage-{'a' * 32}", {"backtest": {}}, session=None)

    assert error.value.status_code == 400


def test_failed_bundle_save_preserves_existing_config_and_override(tmp_path, monkeypatch) -> None:
    """Preparation failure must leave the previously published PB8 bundle unchanged."""
    configs, _v7_configs, _queue, _logs = _patch_roots(tmp_path, monkeypatch)
    target = configs / "demo"
    target.mkdir(parents=True)
    original_config = {"backtest": {}, "coin_overrides": {"HYPE": {"override_config_path": "HYPE.json"}}}
    original_override = {"bot": {"long": {"risk": {"n_positions": 1}}}}
    (target / "backtest.json").write_text(json.dumps(original_config), encoding="utf-8")
    (target / "HYPE.json").write_text(json.dumps(original_override), encoding="utf-8")
    monkeypatch.setattr(backtest_v8, "prepare_pb8_config", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("invalid")))

    with pytest.raises(RuntimeError, match="invalid"):
        backtest_v8.save_config(
            "demo",
            {"config": original_config, "override_configs": {"HYPE.json": {"bot": {"long": {}}}}},
            session=None,
        )

    assert json.loads((target / "backtest.json").read_text(encoding="utf-8")) == original_config
    assert json.loads((target / "HYPE.json").read_text(encoding="utf-8")) == original_override
    assert not list(configs.glob(".demo.*"))


def test_config_lock_recovers_interrupted_directory_swap(tmp_path, monkeypatch) -> None:
    """The next config operation must restore a backup left by a process crash mid-publish."""
    configs, _v7_configs, _queue, _logs = _patch_roots(tmp_path, monkeypatch)
    configs.mkdir(parents=True)
    backup = configs / f".demo.backup-{'a' * 32}"
    backup.mkdir()
    (backup / "backtest.json").write_text('{"backtest": {}}', encoding="utf-8")
    monkeypatch.setattr(backtest_v8, "_log", lambda *_args, **_kwargs: None)

    with backtest_v8._config_lock():
        assert (configs / "demo" / "backtest.json").is_file()

    assert not backup.exists()


def test_main_page_renders_shared_editor_without_exposing_session_token(monkeypatch) -> None:
    """The V8 route must render the V7 editor template with V8 route metadata and cookie auth."""
    monkeypatch.setattr(backtest_v8, "PBGUI_VERSION", "v-test")
    monkeypatch.setattr(backtest_v8, "PBGUI_SERIAL", "123")
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "scheme": "https",
            "server": ("example.test", 443),
            "path": "/api/backtest-v8/main_page",
            "root_path": "",
            "query_string": b"",
            "headers": [],
        }
    )

    response = backtest_v8.main_page(request, session=object())
    html = response.body.decode("utf-8")

    assert "PBGui — V8 Backtest" in html
    assert "PBv8 BACKTEST" in html
    assert "current:  BACKTEST_NAV_CURRENT" in html
    assert "backtestEditorAdapter.isV8 ? 'v8_backtest' : 'v7_backtest'" in html
    assert 'var BACKTEST_VERSION = "v8"' in html
    assert 'var API_BASE      = "https://example.test/api/backtest-v8"' in html
    assert 'var TOKEN         = ""' in html
    assert "function showConfigEditor(" in html
    assert "Canonical V8 Config" not in html


def test_add_to_queue_captures_v8_config_snapshot(tmp_path, monkeypatch) -> None:
    """Queued V8 work must remain independent from later editor saves and PB7 state."""
    configs, _v7_configs, queue, _logs = _patch_roots(tmp_path, monkeypatch)
    config_path = configs / "demo" / "backtest.json"
    config_path.parent.mkdir(parents=True)
    config_path.write_text("{}", encoding="utf-8")
    snapshot = {
        "config_version": "v8.0.0",
        "backtest": {"exchanges": ["bybit"], "base_dir": "backtests/pbgui/demo"},
        "coin_overrides": {"HYPE": {"override_config_path": "HYPE.json"}},
    }
    override = config_path.parent / "HYPE.json"
    override.write_text('{"live": {}}', encoding="utf-8")
    monkeypatch.setattr(backtest_v8, "load_pb8_config", lambda _path: snapshot)
    monkeypatch.setattr(
        backtest_v8,
        "save_prepared_pb8_config",
        lambda config, path: Path(path).write_text(json.dumps(config), encoding="utf-8") or config,
    )

    response = backtest_v8.add_to_queue({"name": "demo"}, session=None)

    queue_payload = json.loads((queue / f"{response['filename']}.json").read_text(encoding="utf-8"))
    assert queue_payload["config_snapshot"] == snapshot
    assert queue_payload["exchange"] == ["bybit"]
    assert (queue / "configs" / response["filename"] / "HYPE.json").read_text(encoding="utf-8") == '{"live": {}}'
    assert not (tmp_path / "data" / "bt_v7_queue").exists()


def test_add_to_queue_accepts_shared_editor_inline_result_config(tmp_path, monkeypatch) -> None:
    """Shared multi-rebacktest flows can queue an unsaved canonical V8 config snapshot."""
    _configs, _v7_configs, queue, _logs = _patch_roots(tmp_path, monkeypatch)
    config = {
        "config_version": "v8.0.0",
        "backtest": {"exchanges": ["bybit"]},
        "bot": {"long": {"risk": {"n_positions": 3}}},
    }
    monkeypatch.setattr(backtest_v8, "prepare_pb8_config", lambda value, **_kwargs: value)
    monkeypatch.setattr(
        backtest_v8,
        "save_prepared_pb8_config",
        lambda value, path: Path(path).write_text(json.dumps(value), encoding="utf-8") or value,
    )

    response = backtest_v8.add_to_queue({"name": "result-retest", "config": config}, session=None)
    saved = json.loads((queue / f"{response['filename']}.json").read_text(encoding="utf-8"))

    assert saved["config_snapshot"]["bot"]["long"]["risk"]["n_positions"] == 3
    assert saved["config_snapshot"]["backtest"]["base_dir"] == "backtests/pbgui/result-retest"


def test_worker_launches_pb8_cli_with_queue_snapshot(tmp_path, monkeypatch) -> None:
    """The worker must launch the PB8 CLI from PB8 cwd using its isolated snapshot."""
    _configs, _v7_configs, queue, _logs = _patch_roots(tmp_path, monkeypatch)
    filename = "queue-demo"
    queue.mkdir(parents=True)
    payload = {
        "name": "demo",
        "filename": filename,
        "config_snapshot": {"config_version": "v8.0.0", "backtest": {}},
    }
    (queue / f"{filename}.json").write_text(json.dumps(payload), encoding="utf-8")
    snapshot_path = queue / "configs" / filename / "backtest.json"
    snapshot_path.parent.mkdir(parents=True)
    snapshot_path.write_text(json.dumps(payload["config_snapshot"]), encoding="utf-8")
    pb8_dir = tmp_path / "pb8"
    cli = tmp_path / "venv_pb8" / "bin" / "passivbot"
    pb8_dir.mkdir()
    cli.parent.mkdir(parents=True)
    cli.write_text("", encoding="utf-8")
    captured = {}

    class FakeProcess:
        pid = 4248

    def fake_popen(command, **kwargs):
        captured["command"] = command
        captured["cwd"] = kwargs["cwd"]
        ready = Path(command[5])
        ready.parent.mkdir(parents=True, exist_ok=True)
        ready.write_text("4248\n", encoding="utf-8")
        return FakeProcess()

    monkeypatch.setattr(backtest_v8, "load_pb8_config", lambda path: json.loads(Path(path).read_text(encoding="utf-8")))
    monkeypatch.setattr(backtest_v8, "prepare_pb8_config", lambda config, **_kwargs: config)
    monkeypatch.setattr(
        backtest_v8,
        "save_prepared_pb8_config",
        lambda config, path: Path(path).write_text(json.dumps(config), encoding="utf-8") or config,
    )
    monkeypatch.setattr(
        backtest_v8,
        "pb8_runtime_status",
        lambda: {
            "ready": True,
            "pb8dir": str(pb8_dir),
            "pb8venv": "/venv_pb8/bin/python",
            "cli_file": str(cli),
            "version": "8.0.0",
        },
    )
    monkeypatch.setattr(backtest_v8, "_runtime_commit", lambda _path: "abc123")
    monkeypatch.setattr(backtest_v8, "load_ini_section", lambda _section: {"use_pbgui_market_data": "True"})
    monkeypatch.setattr(backtest_v8, "_get_pbgui_market_data_path", lambda: str(tmp_path / "market-data"))
    monkeypatch.setattr(backtest_v8, "rotate_managed_log_before_open", lambda *_args: None)
    monkeypatch.setattr(backtest_v8.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(backtest_v8.psutil, "Process", lambda _pid: type("Proc", (), {"create_time": lambda self: 123.0})())
    monkeypatch.setattr(backtest_v8, "_log", lambda *_args, **_kwargs: None)

    backtest_v8.BacktestV8Worker().launch(filename)

    assert captured["command"][0] == "/venv_pb8/bin/python"
    assert captured["command"][2] == "backtest"
    assert captured["command"][6] == str(cli)
    assert captured["command"][-1] == str(snapshot_path.resolve())
    assert captured["cwd"] == str(pb8_dir)
    process_record = json.loads((queue / f"{filename}.pid").read_text(encoding="utf-8"))
    assert process_record == {"pid": 4248, "create_time": 123.0}
    saved_queue = json.loads((queue / f"{filename}.json").read_text(encoding="utf-8"))
    assert saved_queue["pb8_version"] == "8.0.0"
    assert saved_queue["pb8_commit"] == "abc123"
    assert json.loads(snapshot_path.read_text(encoding="utf-8"))["backtest"]["ohlcv_source_dir"] == str(tmp_path / "market-data")
    assert saved_queue["config_snapshot"]["backtest"].get("ohlcv_source_dir") is None


def test_worker_leaves_backtest_queued_while_pb8_update_lock_is_held(tmp_path, monkeypatch) -> None:
    """A PB8 update blocks only new launches and does not corrupt the queued item."""
    _configs, _v7_configs, queue, _logs = _patch_roots(tmp_path, monkeypatch)
    filename = "wait-for-update"
    queue.mkdir(parents=True)
    payload = {"name": "demo", "filename": filename, "config_snapshot": {"backtest": {}}}
    (queue / f"{filename}.json").write_text(json.dumps(payload), encoding="utf-8")
    snapshot = queue / "configs" / filename / "backtest.json"
    snapshot.parent.mkdir(parents=True)
    snapshot.write_text(json.dumps(payload["config_snapshot"]), encoding="utf-8")
    lease = acquire_master_update_lock(tmp_path)
    try:
        with pytest.raises(HTTPException) as error:
            backtest_v8.BacktestV8Worker().launch(filename)
    finally:
        lease.release()

    assert error.value.status_code == 409
    assert json.loads((queue / f"{filename}.json").read_text(encoding="utf-8")) == payload


def test_startup_removes_only_orphan_uuid_queue_snapshots(tmp_path, monkeypatch) -> None:
    """Crash leftovers without queue records are removed while valid and non-UUID directories remain."""
    _configs, _v7_configs, queue, _logs = _patch_roots(tmp_path, monkeypatch)
    orphan = str(uuid.uuid4())
    valid = str(uuid.uuid4())
    for name in (orphan, valid, "manual"):
        (queue / "configs" / name).mkdir(parents=True)
    (queue / f"{valid}.json").write_text(json.dumps({"filename": valid}), encoding="utf-8")

    backtest_v8._cleanup_orphan_queue_snapshots()

    assert not (queue / "configs" / orphan).exists()
    assert (queue / "configs" / valid).is_dir()
    assert (queue / "configs" / "manual").is_dir()


def test_results_are_read_only_from_pb8_root(tmp_path, monkeypatch) -> None:
    """The V8 result parser must not discover analysis files under PB7 roots."""
    pb8_root = tmp_path / "pb8" / "backtests" / "pbgui"
    pb8_analysis = pb8_root / "demo" / "bybit" / "run-1" / "analysis.json"
    pb8_analysis.parent.mkdir(parents=True)
    pb8_analysis.write_text(
        json.dumps(
            {
                "adg_w_usd": 0.02,
                "adg": 0.01,
                "gain_usd": 1.25,
                "drawdown_worst_usd": 0.12,
                "sharpe_ratio_usd": 1.8,
                "final_equity_usd": 6300,
                "equity_balance_diff_neg_max": 0.04,
            }
        ),
        encoding="utf-8",
    )
    (pb8_analysis.parent / "config.json").write_text(
        json.dumps(
            {
                "backtest": {"starting_balance": 5000, "btc_collateral_cap": 0.25, "end_date": "2026-07-01"},
                "bot": {"long": {"risk": {"total_wallet_exposure_limit": 2.0, "n_positions": 6}}},
                "live": {"approved_coins": {"long": ["BTC"], "short": ["ETH"]}},
            }
        ),
        encoding="utf-8",
    )
    pb7_analysis = tmp_path / "pb7" / "backtests" / "pbgui" / "legacy" / "analysis.json"
    pb7_analysis.parent.mkdir(parents=True)
    pb7_analysis.write_text(json.dumps({"adg": 99}), encoding="utf-8")
    monkeypatch.setattr(backtest_v8, "_results_root", lambda: pb8_root)

    results = backtest_v8._list_results()

    assert len(results) == 1
    assert results[0]["config_name"] == "demo"
    assert results[0]["metrics"]["adg_w_usd"] == 0.02
    assert results[0]["adg"] == 0.02
    assert results[0]["gain"] == 1.25
    assert results[0]["drawdown_worst"] == 0.12
    assert results[0]["sharpe_ratio"] == 1.8
    assert results[0]["starting_balance"] == 5000
    assert results[0]["final_balance"] == 6250
    assert results[0]["final_equity"] == 6300
    assert results[0]["equity_balance_diff_neg_max"] == 0.04
    assert results[0]["balance_equity_diff"] == 0.04
    assert results[0]["btc_collateral_cap"] == 0.25
    assert results[0]["end_date"] == "2026-07-01"
    assert results[0]["coins_text"] == "BTC, ETH"
    assert results[0]["twe_long"] == 2.0
    assert results[0]["pos_long"] == 6


def test_results_use_terminal_balance_and_equity_from_gzip_csv(tmp_path, monkeypatch) -> None:
    """PB8 result totals must use the last authoritative compressed CSV values."""
    root = tmp_path / "pb8-results"
    result_dir = root / "demo" / "bybit" / "run-1"
    result_dir.mkdir(parents=True)
    (result_dir / "analysis.json").write_text(json.dumps({"gain_usd": 9.0}), encoding="utf-8")
    (result_dir / "config.json").write_text(json.dumps({"backtest": {"starting_balance": 1000}}), encoding="utf-8")
    with gzip.open(result_dir / "balance_and_equity.csv.gz", "wt", encoding="utf-8", newline="") as handle:
        handle.write("minute,usd_total_balance,usd_total_equity\n0,1000,990\n1,1234.5,1201.25\n")
    monkeypatch.setattr(backtest_v8, "_results_root", lambda: root)

    result = backtest_v8._list_results()[0]

    assert result["final_balance"] == 1234.5
    assert result["final_equity"] == 1201.25


def test_results_fall_back_to_gzip_when_plain_terminal_csv_is_invalid(tmp_path, monkeypatch) -> None:
    """A corrupt preferred CSV must not hide a valid compressed PB8 terminal artifact."""
    root = tmp_path / "pb8-results"
    result_dir = root / "demo" / "bybit" / "run-1"
    result_dir.mkdir(parents=True)
    (result_dir / "analysis.json").write_text("{}", encoding="utf-8")
    (result_dir / "balance_and_equity.csv").write_bytes(b"\xff\xfeinvalid")
    with gzip.open(result_dir / "balance_and_equity.csv.gz", "wt", encoding="utf-8", newline="") as handle:
        handle.write("usd_total_balance,usd_total_equity\n1500,1400\n")
    monkeypatch.setattr(backtest_v8, "_results_root", lambda: root)
    monkeypatch.setattr(backtest_v8, "_log", lambda *_args, **_kwargs: None)

    result = backtest_v8._list_results()[0]

    assert result["final_balance"] == 1500
    assert result["final_equity"] == 1400


def test_results_ignore_incomplete_plain_terminal_row_before_gzip_fallback(tmp_path, monkeypatch) -> None:
    """A truncated final plain row must not make an older pair override a complete gzip artifact."""
    root = tmp_path / "pb8-results"
    result_dir = root / "demo" / "bybit" / "run-1"
    result_dir.mkdir(parents=True)
    (result_dir / "analysis.json").write_text("{}", encoding="utf-8")
    (result_dir / "balance_and_equity.csv").write_text(
        "usd_total_balance,usd_total_equity\n1000,990\n1100,\n",
        encoding="utf-8",
    )
    with gzip.open(result_dir / "balance_and_equity.csv.gz", "wt", encoding="utf-8", newline="") as handle:
        handle.write("usd_total_balance,usd_total_equity\n1500,1400\n")
    monkeypatch.setattr(backtest_v8, "_results_root", lambda: root)

    result = backtest_v8._list_results()[0]

    assert result["final_balance"] == 1500
    assert result["final_equity"] == 1400


def test_result_delete_rejects_root_and_intermediate_directories(tmp_path, monkeypatch) -> None:
    """Result deletion must only accept a leaf directory containing analysis.json."""
    root = tmp_path / "pb8" / "backtests" / "pbgui"
    leaf = root / "demo" / "bybit" / "run-1"
    leaf.mkdir(parents=True)
    (leaf / "analysis.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(backtest_v8, "_results_root", lambda: root)

    for unsafe in (root, root / "demo", root / "demo" / "bybit"):
        with pytest.raises(HTTPException) as error:
            backtest_v8.delete_result(str(unsafe), session=None)
        assert error.value.status_code == 400

    assert leaf.is_dir()
    assert (leaf / "analysis.json").is_file()


def test_result_delete_removes_selected_result_directory(tmp_path, monkeypatch) -> None:
    """Deleting a validated PB8 result must remove only that result directory."""
    root = tmp_path / "pb8" / "backtests" / "pbgui"
    leaf = root / "demo" / "bybit" / "run-1"
    sibling = root / "demo" / "bybit" / "run-2"
    leaf.mkdir(parents=True)
    sibling.mkdir(parents=True)
    (leaf / "analysis.json").write_text("{}", encoding="utf-8")
    (sibling / "analysis.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(backtest_v8, "_results_root", lambda: root)

    response = backtest_v8.delete_result(str(leaf), session=None)

    assert response == {"ok": True}
    assert not leaf.exists()
    assert sibling.is_dir()


def test_result_files_include_nested_plots_without_allowing_traversal(tmp_path, monkeypatch) -> None:
    """The shared PB7 result panel can discover safe nested PB8 plot images."""
    root = tmp_path / "pb8" / "backtests" / "pbgui"
    leaf = root / "demo" / "bybit" / "run-1"
    plot = leaf / "fills_plots" / "BTC.png"
    plot.parent.mkdir(parents=True)
    (leaf / "analysis.json").write_text("{}", encoding="utf-8")
    plot.write_bytes(b"png")
    monkeypatch.setattr(backtest_v8, "_results_root", lambda: root)

    files = backtest_v8.get_result_files(str(leaf), session=None)
    response = backtest_v8.get_result_image(str(leaf), "fills_plots/BTC.png", session=None)

    assert "fills_plots/BTC.png" in files["files"]
    assert Path(response.path) == plot
    with pytest.raises(HTTPException) as error:
        backtest_v8.get_result_image(str(leaf), "../outside.png", session=None)
    assert error.value.status_code == 400


def test_override_param_metadata_preserves_v8_leaf_types(monkeypatch) -> None:
    """Shared override controls receive enough type data for booleans and strings."""
    monkeypatch.setattr(
        backtest_v8,
        "get_pb8_template_config",
        lambda: {
            "bot": {"long": {"hsl": {"enabled": False, "restart_after_red_policy": "threshold"}}, "short": {}},
            "live": {"leverage": 10},
        },
    )

    params = backtest_v8.get_override_params(session=None)["params"]

    assert params["bot"]["long"]["hsl.enabled"] == {"type": "boolean", "default": False}
    assert params["bot"]["long"]["hsl.restart_after_red_policy"] == {"type": "string", "default": "threshold"}
    assert params["live"]["leverage"] == {"type": "number", "default": 10}


def test_result_metrics_come_from_installed_pb8_runtime(monkeypatch) -> None:
    """The editor should receive the current runtime's accepted metric names."""
    monkeypatch.setattr(
        backtest_v8,
        "get_pb8_result_metrics",
        lambda: ["adg", "hard_stop_triggers_per_year"],
    )

    assert backtest_v8.get_result_metrics(session=None) == {
        "metrics": ["adg", "hard_stop_triggers_per_year"]
    }


def test_second_start_cannot_launch_same_queue_item_twice(tmp_path, monkeypatch) -> None:
    """A persisted process record must make a concurrent or repeated start return 409."""
    _configs, _v7_configs, queue, _logs = _patch_roots(tmp_path, monkeypatch)
    filename = "queue-once"
    queue.mkdir(parents=True)
    payload = {"name": "demo", "filename": filename, "config_snapshot": {"backtest": {}}}
    (queue / f"{filename}.json").write_text(json.dumps(payload), encoding="utf-8")
    snapshot = queue / "configs" / filename / "backtest.json"
    snapshot.parent.mkdir(parents=True)
    snapshot.write_text(json.dumps(payload["config_snapshot"]), encoding="utf-8")
    pb8_dir = tmp_path / "pb8"
    pb8_dir.mkdir()
    cli = tmp_path / "venv_pb8" / "bin" / "passivbot"
    cli.parent.mkdir(parents=True)
    cli.write_text("", encoding="utf-8")
    launches = []

    class FakeProcessHandle:
        pid = 4250

    class FakePsutilProcess:
        def __init__(self, _pid):
            pass

        def create_time(self):
            return 123.0

        def cmdline(self):
            return launches[0]

    def fake_popen(command, **_kwargs):
        launches.append(command)
        ready = Path(command[5])
        ready.parent.mkdir(parents=True, exist_ok=True)
        ready.write_text("4250\n", encoding="utf-8")
        return FakeProcessHandle()

    monkeypatch.setattr(backtest_v8, "load_pb8_config", lambda _path: payload["config_snapshot"])
    monkeypatch.setattr(backtest_v8, "prepare_pb8_config", lambda config, **_kwargs: config)
    monkeypatch.setattr(
        backtest_v8,
        "save_prepared_pb8_config",
        lambda config, path: Path(path).write_text(json.dumps(config), encoding="utf-8") or config,
    )
    monkeypatch.setattr(
        backtest_v8,
        "pb8_runtime_status",
        lambda: {
            "ready": True,
            "pb8dir": str(pb8_dir),
            "pb8venv": "/venv_pb8/bin/python",
            "cli_file": str(cli),
            "version": "8.0.0",
        },
    )
    monkeypatch.setattr(backtest_v8, "_runtime_commit", lambda _path: "abc123")
    monkeypatch.setattr(backtest_v8, "rotate_managed_log_before_open", lambda *_args: None)
    monkeypatch.setattr(backtest_v8.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(backtest_v8.psutil, "Process", FakePsutilProcess)
    monkeypatch.setattr(backtest_v8, "_log", lambda *_args, **_kwargs: None)

    worker = backtest_v8.BacktestV8Worker()
    worker.launch(filename)
    with pytest.raises(HTTPException) as error:
        worker.launch(filename)

    assert error.value.status_code == 409
    assert len(launches) == 1


def test_backtest_settings_share_the_pb7_configuration(monkeypatch) -> None:
    """PB8 must read and write the one existing PB7 Backtest queue settings section."""
    saved = {}
    monkeypatch.setattr(backtest_v8.multiprocessing, "cpu_count", lambda: 16)
    monkeypatch.setattr(
        backtest_v8,
        "load_ini_section",
        lambda section: {
            "autostart": "True",
            "cpu": "8",
            "use_pbgui_market_data": "True",
            "hlcvs_cleanup_enabled": "True",
            "hlcvs_cleanup_days": "9",
            "hlcvs_cleanup_interval_h": "12",
        }
        if section == "backtest_v7"
        else pytest.fail(f"Unexpected settings section: {section}"),
    )
    monkeypatch.setattr(backtest_v8, "save_ini_section", lambda section, values: saved.update(section=section, values=values))

    settings = backtest_v8.get_settings(None)
    assert settings == {
        "autostart": True,
        "cpu": 8,
        "cpu_max": 16,
        "use_pbgui_market_data": True,
        "hsl_signal_modes": ["coin", "pside", "unified"],
        "hlcvs_cleanup_enabled": True,
        "hlcvs_cleanup_days": 9,
        "hlcvs_cleanup_interval_h": 12,
    }

    backtest_v8.update_settings(
        {
            "autostart": False,
            "cpu": 6,
            "use_pbgui_market_data": False,
            "hlcvs_cleanup_enabled": False,
            "hlcvs_cleanup_days": 7,
            "hlcvs_cleanup_interval_h": 24,
        },
        None,
    )
    assert saved["section"] == "backtest_v7"
    assert backtest_v8._QUEUE_SETTINGS_SECTION == "backtest_v7"
    source = Path(backtest_v8.__file__).read_text(encoding="utf-8")
    assert 'load_ini_section("backtest_v8")' not in source
    assert 'save_ini_section("backtest_v8"' not in source


def test_pb8_cache_cleanup_preserves_active_foreign_and_unknown_materialized_locks(tmp_path, monkeypatch) -> None:
    """PB8 cleanup removes stale data but never deletes materialized runs with unsafe locks."""
    pb8_dir = tmp_path / "pb8"
    hlcvs_root = pb8_dir / "caches" / "hlcvs_data"
    materialized_root = pb8_dir / "caches" / "ohlcvs" / "materialized"
    hlcvs_root.mkdir(parents=True)
    materialized_root.mkdir(parents=True)
    old_time = time.time() - 3 * 86400

    def old_directory(root: Path, name: str, lock_payload=None) -> Path:
        directory = root / name
        directory.mkdir()
        (directory / "payload.dat").write_bytes(b"data")
        if lock_payload is not None:
            lock_text = lock_payload if isinstance(lock_payload, str) else json.dumps(lock_payload)
            (directory / ".materialized.lock.json").write_text(lock_text, encoding="utf-8")
        os.utime(directory, (old_time, old_time))
        return directory

    old_directory(hlcvs_root, "old-dataset")
    unlocked = old_directory(materialized_root, "unlocked")
    stale = old_directory(
        materialized_root,
        "stale",
        {"pid": 333, "hostname": backtest_v8.socket.gethostname()},
    )
    active = old_directory(
        materialized_root,
        "active",
        {"pid": 111, "hostname": backtest_v8.socket.gethostname()},
    )
    foreign = old_directory(materialized_root, "foreign", {"pid": 222, "hostname": "another-host"})
    malformed = old_directory(materialized_root, "malformed", "{not-json")

    monkeypatch.setattr(backtest_v8, "pb8_runtime_status", lambda: {"pb8dir": str(pb8_dir)})
    monkeypatch.setattr(backtest_v8.psutil, "pid_exists", lambda pid: pid == 111)

    result = backtest_v8._cleanup_pb8_caches(1)

    assert result == {"removed": 3, "freed_mb": 0, "errors": 0, "skipped_locked": 3}
    assert not unlocked.exists()
    assert not stale.exists()
    assert active.exists()
    assert foreign.exists()
    assert malformed.exists()


def test_pb8_cache_cleanup_respects_active_materialized_operation_lock(tmp_path, monkeypatch) -> None:
    """PBGui must not race PB8 while its root-level materialized operation lock is active."""
    pb8_dir = tmp_path / "pb8"
    materialized_root = pb8_dir / "caches" / "ohlcvs" / "materialized"
    run_dir = materialized_root / "old-run"
    operation_lock = materialized_root / ".materialized.op.lock"
    run_dir.mkdir(parents=True)
    operation_lock.mkdir()
    (operation_lock / "lock.json").write_text(
        json.dumps({"pid": 111, "hostname": backtest_v8.socket.gethostname()}),
        encoding="utf-8",
    )
    old_time = time.time() - 3 * 86400
    os.utime(run_dir, (old_time, old_time))
    monkeypatch.setattr(backtest_v8, "pb8_runtime_status", lambda: {"pb8dir": str(pb8_dir)})
    monkeypatch.setattr(backtest_v8.psutil, "pid_exists", lambda pid: pid == 111)

    result = backtest_v8._cleanup_pb8_caches(1)

    assert result["removed"] == 0
    assert result["skipped_locked"] == 1
    assert run_dir.exists()
    assert operation_lock.exists()
