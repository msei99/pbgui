"""Regression tests for PB8 backtest persistence and process isolation."""

from __future__ import annotations

import copy
import gzip
import json
import os
import threading
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from starlette.requests import Request

import backtest_result_index
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
    assert optimize_payload["override_configs"] == {}

    queue_id = backtest_v8.create_queue_draft(
        {"items": [{
            "name": "candidate",
            "config": optimize_payload["config"],
            "preserve_timerange": True,
            "preserve_exchanges": True,
        }]},
        session=None,
    )["draft_id"]
    queue_payload = backtest_v8.get_queue_draft(queue_id, session=None)
    assert queue_payload["items"] == [{
        "name": "candidate",
        "config": optimize_payload["config"],
        "override_configs": {},
        "preserve_timerange": True,
        "preserve_exchanges": True,
    }]

    with pytest.raises(HTTPException) as error:
        backtest_v8.create_queue_draft({"items": []}, session=None)
    assert error.value.status_code == 422

    with pytest.raises(HTTPException) as error:
        backtest_v8.create_queue_draft(
            {"items": [{"config": optimize_payload["config"], "preserve_exchanges": "yes"}]},
            session=None,
        )
    assert error.value.status_code == 422

    with pytest.raises(HTTPException) as error:
        backtest_v8.create_queue_draft(
            {"items": [{"config": optimize_payload["config"], "preserve_timerange": "yes"}]},
            session=None,
        )
    assert error.value.status_code == 422


@pytest.mark.parametrize(
    ("started_at", "expected"),
    [
        (None, "queued"),
        (99.0, "running"),
        (95.0, "error"),
        (90.0, "error"),
        ("invalid", "error"),
    ],
)
def test_queue_status_does_not_flash_error_while_runner_publishes_pid(
    monkeypatch, started_at, expected: str
) -> None:
    """Only a freshly started queue item remains running during the bounded PID publication gap."""
    monkeypatch.setattr(backtest_v8, "_read_process_record", lambda _filename: None)
    monkeypatch.setattr(backtest_v8, "_read_runner_state", lambda _filename: None)
    monkeypatch.setattr(backtest_v8.time, "time", lambda: 100.0)

    data = {"filename": "job"}
    if started_at is not None:
        data["started_at"] = started_at
    assert backtest_v8._queue_status(data) == (expected, None)


def test_pb8_legacy_results_are_read_only_safe_and_exclude_managed_root(
    tmp_path: Path, monkeypatch
) -> None:
    """PB8 should discover valid legacy artifacts without widening managed deletion."""
    backtests = tmp_path / "pb8" / "backtests"
    legacy_result = backtests / "combined" / "2026-08-27T01_00_00"
    managed_result = backtests / "pbgui" / "managed" / "combined" / "2026-08-27T02_00_00"
    invalid_result = backtests / "binance" / "invalid"
    for result_dir in (legacy_result, managed_result, invalid_result):
        result_dir.mkdir(parents=True)
        (result_dir / "analysis.json").write_text(json.dumps({"gain_usd": 1.2}), encoding="utf-8")
    config = {
        "config_version": "v8.0.0",
        "backtest": {
            "base_dir": "backtests",
            "exchanges": ["binance", "bybit"],
            "starting_balance": 1000,
        },
        "live": {
            "strategy_kind": "trailing_martingale",
            "approved_coins": {"long": ["LTC"], "short": []},
        },
        "bot": {
            "long": {"risk": {"n_positions": 1, "total_wallet_exposure_limit": 1.0}},
            "short": {"risk": {"n_positions": 0, "total_wallet_exposure_limit": 0.0}},
        },
    }
    for result_dir in (legacy_result, managed_result):
        (result_dir / "config.json").write_text(json.dumps(config), encoding="utf-8")
    (invalid_result / "config.json").write_text(
        json.dumps({"config_version": "v7.0.0", "backtest": {}}), encoding="utf-8"
    )
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "analysis.json").write_text("{}", encoding="utf-8")
    try:
        (backtests / "linked").symlink_to(outside, target_is_directory=True)
    except OSError:
        pass
    monkeypatch.setattr(backtest_v8, "_backtests_root", lambda: backtests)

    payload = backtest_v8.list_legacy_results(session=None)

    assert payload["read_only"] is True
    assert len(payload["results"]) == 1
    result = payload["results"][0]
    assert result["path"] == str(legacy_result)
    assert result["config_name"] == "Legacy combined"
    assert result["exchange_dir"] == "combined"
    assert result["backtest_version"] == "v8"
    assert result["strategy"] == "trailing_martingale"
    assert backtest_v8.get_result_analysis(str(legacy_result), session=None)["gain_usd"] == 1.2
    with pytest.raises(HTTPException) as delete_error:
        backtest_v8.delete_result(str(legacy_result), session=None)
    assert delete_error.value.status_code == 400
    with pytest.raises(HTTPException) as draft_error:
        backtest_v8.create_result_run_draft({"path": str(legacy_result)}, session=None)
    assert draft_error.value.status_code == 400
    assert legacy_result.is_dir()


def test_build_optimize_preset_from_pb8_backtest_result(tmp_path: Path, monkeypatch) -> None:
    """PB8 backtest results generate nested PB8 optimize bounds and scoring."""
    result_dir = tmp_path / "demo" / "result-1"
    result_dir.mkdir(parents=True)
    config = {
        "config_version": "v8.0.0",
        "backtest": {"base_dir": "demo"},
        "bot": {
            "long": {"risk": {"n_positions": 3}},
            "short": {"risk": {"n_positions": 0}},
        },
        "optimize": {
            "bounds": {"long": {"risk": {"n_positions": [1, 5, 1]}}},
            "scoring": [{"metric": "adg", "goal": "maximize"}],
            "limits": [],
        },
    }
    (result_dir / "config.json").write_text(json.dumps(config), encoding="utf-8")
    (result_dir / "analysis.json").write_text(json.dumps({"adg": 0.02}), encoding="utf-8")
    monkeypatch.setattr(backtest_v8, "_resolve_result_dir", lambda _path, **_kwargs: result_dir)
    monkeypatch.setattr(
        backtest_v8,
        "get_pb8_optimize_metadata",
        lambda: {"scoring_goals": {"adg": "maximize"}},
    )

    result = backtest_v8.build_result_optimize_preset(
        {
            "result_path": str(result_dir),
            "preset": {
                "direction": "Balanced (keep run scoring)",
                "bounds_window_pct": 10,
                "show_near_bounds": True,
            },
        },
        session=None,
    )

    assert result["ok"] is True
    assert result["preset_config"]["config_version"] == "v8.0.0"
    assert result["preset_config"]["optimize"]["bounds"]["long"]["risk"]["n_positions"] == [3.0, 4.0, 1.0]
    assert result["near_bounds_count"] == 0


def test_result_run_draft_reuses_canonical_result_without_pb8_prepare(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Add to Run stores the emitted PB8 result directly instead of re-preparing it."""

    result_dir = tmp_path / "result-1"
    result_dir.mkdir()
    config = {"live": {"user": "alice"}, "pbgui": {"enabled_on": "old-host"}}
    (result_dir / "config.json").write_text(json.dumps(config), encoding="utf-8")
    monkeypatch.setattr(backtest_v8, "_resolve_result_dir", lambda _path, **_kwargs: result_dir)
    captured = {}

    def store(candidate: dict) -> dict:
        captured["config"] = candidate
        return {"draft_id": "draft-1", "expires_in": 300}

    monkeypatch.setattr(backtest_v8, "store_v8_editor_draft", store)

    result = backtest_v8.create_result_run_draft({"path": "result-1"}, session=None)

    assert result == {"draft_id": "draft-1", "expires_in": 300, "name": "result-1"}
    assert captured["config"]["live"]["user"] == "alice"
    assert captured["config"]["pbgui"] == {"enabled_on": "disabled", "runtime": "pb8"}


def test_shared_backtest_refine_builder_routes_pb8_actions_to_pb8() -> None:
    """The shared result builder must save, queue, and open PB8 presets in PB8 APIs."""
    source = (Path(__file__).parents[1] / "frontend" / "js" / "optimize_preset_builder.js").read_text(encoding="utf-8")
    page = (Path(__file__).parents[1] / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")

    assert "'/api/optimize-'" in source
    assert "'/api/backtest-'" in source
    assert "if (String(token || '').trim()) headers.Authorization" in source
    assert "saveOptimizePresetConfig(TOKEN, name, config, BACKTEST_VERSION)" in page
    assert "queueOptimizePreset(TOKEN, name, BACKTEST_VERSION)" in page
    assert "openOptimizeSeedDraft(TOKEN, config, name, BACKTEST_VERSION)" in page
    assert "optimize_preset_builder.js?v=3" in page


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


def test_run_to_backtest_draft_preserves_sparse_override_bundle() -> None:
    """PB8 Run handoffs retain referenced override files as isolated copies."""
    backtest_v8._opt_draft_store.clear()
    config = {
        "coin_overrides": {"BTC": {"override_config_path": "BTC.json"}},
        "bot": {"long": {"risk": {"n_positions": 3}}},
    }
    override = {"bot": {"long": {"risk": {"n_positions": 1}}}}

    draft_id = backtest_v8.create_optimize_draft(
        {"config": config, "override_configs": {"BTC.json": override}},
        session=None,
    )["draft_id"]
    override["bot"]["long"]["risk"]["n_positions"] = 99
    payload = backtest_v8.get_optimize_draft(draft_id, session=None)

    assert payload["override_configs"]["BTC.json"]["bot"]["long"]["risk"]["n_positions"] == 1


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


def test_migrate_v7_keeps_source_and_opens_unsaved_draft(tmp_path, monkeypatch) -> None:
    """Successful conversion must leave PB7 unchanged and publish nothing before manual Save."""
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

    monkeypatch.setattr(backtest_v8, "migrate_pb7_config", fake_migrate)
    monkeypatch.setattr(
        backtest_v8,
        "save_prepared_pb8_config",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("Convert must not save")),
    )
    monkeypatch.setattr(backtest_v8, "_log", lambda *_args, **_kwargs: None)

    response = backtest_v8.migrate_v7(
        {"source_name": "demo", "target_name": "demo_v8"},
        session=None,
    )

    assert response["name"] == "demo_v8"
    assert response["editor"] == "backtest"
    assert response["draft_id"]
    assert json.loads(source.read_text(encoding="utf-8")) == source_payload
    draft = backtest_v8.get_optimize_draft(response["draft_id"], session=None)
    assert draft["migration_report"]["status"] == "ok"
    assert draft["migration_report"]["pbgui_source_adjustments"] == ["pbgui", "live.base_config_path"]
    assert draft["config"]["backtest"]["base_dir"] == "backtests/pbgui/demo_v8"
    assert not (configs / "demo_v8").exists()


def test_migrate_v7_postprocesses_optimizer_safety_before_publish(tmp_path, monkeypatch) -> None:
    """Backtest conversion must publish the same optimizer-safe shape as the Optimize editor."""
    configs, v7_configs, _queue, _logs = _patch_roots(tmp_path, monkeypatch)
    source = v7_configs / "demo" / "backtest.json"
    source.parent.mkdir(parents=True)
    source.write_text(
        json.dumps(
            {
                "bot": {
                    "long": {
                        "risk_wel_enforcer_threshold": 0.938,
                        "risk_twel_enforcer_threshold": 1.01,
                    },
                    "short": {},
                }
            }
        ),
        encoding="utf-8",
    )
    migrated = {
        "config_version": "v8.0.0",
        "backtest": {},
        "live": {"strategy_kind": "trailing_grid_v7"},
        "bot": {
            "long": {
                "risk": {
                    "n_positions": 4,
                    "total_wallet_exposure_limit": 1.6,
                    "position_exposure_enforcer_enabled": False,
                    "position_exposure_enforcer_threshold": 0.938,
                    "total_exposure_enforcer_enabled": False,
                    "total_exposure_enforcer_threshold": 1.01,
                },
                "hsl": {"no_restart_drawdown_threshold": 0.5},
            },
            "short": {
                "risk": {"n_positions": 0, "total_wallet_exposure_limit": 0.0},
                "hsl": {"no_restart_drawdown_threshold": 0.5},
            },
        },
        "optimize": {
            "enable_overrides": ["lossless_close_trailing", "forward_tp_grid"],
            "fixed_params": [],
            "fixed_runtime_overrides": {
                "bot.long.hsl_no_restart_drawdown_threshold": 1,
                "bot.short.hsl_no_restart_drawdown_threshold": 1,
            },
            "bounds": {},
            "scoring": [],
            "limits": [],
        },
    }

    def fake_migrate(_source_path, output_path, **_kwargs):
        Path(output_path).write_text(json.dumps(migrated), encoding="utf-8")
        return {
            "report": {"output_written": True, "status": "ok", "manual_review_fields": []},
            "config": migrated,
        }

    def fake_save(config, path):
        Path(path).write_text(json.dumps(config), encoding="utf-8")
        return config

    validated = []
    monkeypatch.setattr(backtest_v8, "migrate_pb7_config", fake_migrate)
    monkeypatch.setattr(backtest_v8, "save_prepared_pb8_config", fake_save)
    monkeypatch.setattr(
        backtest_v8,
        "validate_pb8_optimizer_overrides",
        lambda config, **_kwargs: validated.append(copy.deepcopy(config)),
    )
    monkeypatch.setattr(backtest_v8, "_log", lambda *_args, **_kwargs: None)

    response = backtest_v8.migrate_v7(
        {"source_name": "demo", "target_name": "demo_v8"},
        session=None,
    )

    draft = backtest_v8.get_optimize_draft(response["draft_id"], session=None)
    saved = draft["config"]
    assert saved["optimize"]["enable_overrides"] == ["forward_tp_grid"]
    assert saved["optimize"]["fixed_runtime_overrides"] == {
        "bot.long.hsl.no_restart_drawdown_threshold": 1,
        "bot.short.hsl.no_restart_drawdown_threshold": 1,
    }
    assert saved["optimize"]["fixed_params"] == ["bot.short"]
    assert saved["bot"]["long"]["risk"]["position_exposure_enforcer_enabled"] is True
    assert saved["bot"]["long"]["risk"]["total_exposure_enforcer_enabled"] is True
    assert validated[0]["optimize"]["enable_overrides"] == ["forward_tp_grid"]
    assert response["report"]["status"] == "ok_with_adjustments"
    assert response["report"]["manual_review_required"] is False
    assert any(
        "lossless_close_trailing" in item["detail"]
        for item in response["report"]["pbgui_post_migration_adjustments"]
    )
    assert not (configs / "demo_v8").exists()


def test_migration_sanitizer_resolves_context_safe_legacy_fields() -> None:
    """Backtest-only migration drops live execution gates and uses canonical reducer aliases."""
    source = {
        "live": {
            "empty_means_all_approved": False,
            "price_distance_threshold": 0.006,
        },
        "bot": {
            "long": {"filter_volatility_drop_pct": 0.0},
            "short": {"filter_volatility_drop_pct": 0},
        },
        "backtest": {
            "aggregate": {"default": "mean"},
            "exchange": "binance",
            "exchanges": ["binance", "bybit"],
        },
        "optimize": {"bounds": {"long_filter_volatility_drop_pct": [0.0, 0.0]}},
    }

    backtest, adjustments = backtest_v8._sanitize_v7_migration_payload(source, "backtest_result")
    run, _run_adjustments = backtest_v8._sanitize_v7_migration_payload(source, "run_config")
    run, churn_plan, churn_adjustments = backtest_v8.extract_legacy_churn_gate(run)

    assert "initial_entry_exec_max_market_dist_pct" not in backtest["live"]
    assert "price_distance_threshold" not in backtest["live"]
    assert "empty_means_all_approved" not in backtest["live"]
    assert "initial_entry_exec_max_market_dist_pct" not in run["live"]
    assert "price_distance_threshold" not in run["live"]
    assert churn_plan == {
        "source_field": "live.price_distance_threshold",
        "value": 0.006,
        "explicit_churn_keys": [],
    }
    assert churn_adjustments == ["live.price_distance_threshold -> PB8 canonical churn-gate migration"]
    assert "empty_means_all_approved" not in run["live"]
    assert "filter_volatility_drop_pct" not in run["bot"]["long"]
    assert "filter_volatility_drop_pct" not in run["bot"]["short"]
    assert run["backtest"]["reducer"] == {"default": "mean"}
    assert "aggregate" not in run["backtest"]
    assert run["optimize"] == source["optimize"]
    assert backtest["backtest"]["reducer"] == {"default": "mean"}
    assert "aggregate" not in backtest["backtest"]
    assert "exchange" not in backtest["backtest"]
    assert "live.price_distance_threshold (live-only; omitted for backtest)" in adjustments
    assert "backtest.aggregate -> backtest.reducer" in adjustments


def test_normalize_config_filters_suite_scenarios_to_selected_exchanges() -> None:
    """Removing a base exchange must remove or narrow stale PB8 suite scenarios."""
    config = {
        "backtest": {
            "exchanges": ["hyperliquid"],
            "suite_enabled": True,
            "scenarios": [
                {"label": "hyperliquid", "exchanges": ["hyperliquid"]},
                {"label": "bybit", "exchanges": ["bybit"]},
                {"label": "mixed", "exchanges": ["bybit", "hyperliquid"]},
                {"label": "inherits-base"},
            ],
        }
    }

    normalized = backtest_v8._normalize_config(config, "suite")

    assert normalized["backtest"]["scenarios"] == [
        {"label": "hyperliquid", "exchanges": ["hyperliquid"]},
        {"label": "mixed", "exchanges": ["hyperliquid"]},
        {"label": "inherits-base"},
    ]
    assert config["backtest"]["scenarios"][1]["exchanges"] == ["bybit"]


def test_normalize_config_preserves_malformed_suite_exchanges_for_pb8_validation() -> None:
    """PBGui must not hide malformed exchange values while removing stale valid names."""
    config = {
        "backtest": {
            "exchanges": ["hyperliquid", 42, ""],
            "suite_enabled": True,
            "scenarios": [
                {"label": "invalid", "exchanges": ["bybit", 42, ""]},
            ],
        }
    }

    normalized = backtest_v8._normalize_config(config, "suite")

    assert normalized["backtest"]["exchanges"] == ["hyperliquid", 42, ""]
    assert normalized["backtest"]["scenarios"] == [
        {"label": "invalid", "exchanges": [42, ""]},
    ]


def test_migration_review_marks_only_existing_canonical_fields() -> None:
    """Review metadata must never recreate retired V7 paths in a V8 draft."""
    config = {"bot": {"long": {"example": 1}, "short": {}}, "live": {}}

    review, param_status = backtest_v8._migration_review_config(
        config,
        {"bot.long.example": 42, "live.retired_parameter": 0.1},
    )

    assert review["bot"]["long"]["example"] == 1
    assert "retired_parameter" not in review["live"]
    assert param_status == {"long": {"example": "review"}, "short": {}}


def test_postprocess_review_fields_block_even_when_optimize_findings_are_context_filtered() -> None:
    """PBGui safety conflicts must block a Backtest bundle that could later crash Optimize."""
    field = "optimize.fixed_runtime_overrides.bot.long.hsl_no_restart_drawdown_threshold"
    review, _values = backtest_v8._migration_review_payload(
        {
            "status": "manual_review_required",
            "manual_review_fields": [field],
            "pbgui_post_migration_review_fields": [field],
        },
        {},
        "backtest_config",
    )

    assert review["manual_review_required"] is True
    assert review["manual_review_fields"] == [field]


def test_success_report_keeps_filtered_official_findings_separate() -> None:
    """Published context status must agree with its fields without discarding official provenance."""
    report = {
        "status": "unsafe_manual_review_output_written",
        "manual_review_required": True,
        "manual_review_fields": ["live.execution_delay_seconds"],
        "dropped_unsupported_fields": [],
    }
    review = {
        "manual_review_fields": [],
        "dropped_unsupported_fields": [],
    }

    contextual = backtest_v8._successful_migration_report(report, review)

    assert contextual["status"] == "ok"
    assert contextual["manual_review_required"] is False
    assert contextual["manual_review_fields"] == []
    assert contextual["official_review"]["manual_review_fields"] == ["live.execution_delay_seconds"]


def test_legacy_churn_alias_conflict_is_rejected_before_migration() -> None:
    """Two contradictory retired distance fields must not be resolved silently."""
    with pytest.raises(ValueError, match="conflicts with"):
        backtest_v8.extract_legacy_churn_gate(
            {
                "live": {
                    "price_distance_threshold": 0.006,
                    "initial_entry_exec_max_market_dist_pct": 0.005,
                }
            }
        )


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
    monkeypatch.setattr(
        backtest_v8,
        "save_prepared_pb8_config",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("Convert must not save")),
    )
    monkeypatch.setattr(backtest_v8, "_log", lambda *_args, **_kwargs: None)
    body = {"source_type": source_type, "source_name": "demo", "target_name": f"{source_type}_v8"}
    if source_type == "backtest_result":
        body["source_path"] = str(source_dir)

    response = backtest_v8.migrate_v7(body, session=None)

    assert response["name"] == f"{source_type}_v8"
    if source_type == "run_config":
        assert response["editor"] == "run"
        assert response["draft_id"]
        assert not (configs / f"{source_type}_v8").exists()
    else:
        assert response["editor"] == "backtest"
        assert response["draft_id"]
        assert not (configs / f"{source_type}_v8").exists()


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
    monkeypatch.setattr(
        backtest_v8,
        "save_prepared_pb8_config",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("Convert must not save")),
    )
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
    draft = backtest_v8.get_optimize_draft(response["draft_id"], session=None)
    assert draft["migration_report"]["pbgui_result_fee_adjustments"] == response["report"]["pbgui_result_fee_adjustments"]
    assert not (configs / "demo_v8").exists()


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


def test_migrate_v7_existing_target_still_opens_unsaved_draft(tmp_path, monkeypatch) -> None:
    """Convert must not open or overwrite an existing target before the user reviews the draft."""
    configs, v7_configs, _queue, _logs = _patch_roots(tmp_path, monkeypatch)
    source = v7_configs / "demo" / "backtest.json"
    source.parent.mkdir(parents=True)
    source.write_text("{}", encoding="utf-8")
    target = configs / "demo_v8"
    target.mkdir(parents=True)
    (target / "backtest.json").write_text('{"existing": true}', encoding="utf-8")
    migrated = {"config_version": "v8.0.0", "backtest": {}}
    monkeypatch.setattr(
        backtest_v8,
        "migrate_pb7_config",
        lambda *_args, **_kwargs: {
            "report": {"output_written": True, "status": "ok", "manual_review_fields": []},
            "config": migrated,
        },
    )
    monkeypatch.setattr(backtest_v8, "_log", lambda *_args, **_kwargs: None)

    response = backtest_v8.migrate_v7(
        {"source_name": "demo", "target_name": "demo_v8"},
        session=None,
    )

    assert response["editor"] == "backtest"
    assert response["draft_id"]
    assert (target / "backtest.json").read_text(encoding="utf-8") == '{"existing": true}'


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
    assert error.value.detail["code"] == "migration_manual_review"
    assert error.value.detail["report"]["manual_review_fields"] == ["bot.long.example"]
    assert not (configs / "demo_v8").exists()
    assert not list(configs.glob(".migrate-*"))


def test_manual_review_output_is_not_published_as_runnable_config(tmp_path, monkeypatch) -> None:
    """Best-effort PB8 output opens as a draft while remaining unpublished."""
    configs, v7_configs, _queue, _logs = _patch_roots(tmp_path, monkeypatch)
    source = v7_configs / "demo" / "backtest.json"
    source.parent.mkdir(parents=True)
    source.write_text(json.dumps({"bot": {"long": {"example": 42}}}), encoding="utf-8")
    monkeypatch.setattr(
        backtest_v8,
        "migrate_pb7_config",
        lambda *_args, **_kwargs: {
            "report": {"output_written": True, "manual_review_fields": ["bot.long.example"]},
            "config": {"config_version": "v8.0.0", "backtest": {}},
        },
    )

    result = backtest_v8.migrate_v7(
        {
            "source_name": "demo",
            "target_name": "demo_v8",
            "allow_manual_review_output": True,
        },
        session=None,
    )
    draft = backtest_v8.get_optimize_draft(result["draft_id"], session=None)

    assert result["review_required"] is True
    assert draft["config"]["config_version"] == "v8.0.0"
    assert "bot" not in draft["config"]
    assert draft["param_status"] == {"long": {}, "short": {}}
    assert draft["migration_report"]["manual_review_fields"] == ["bot.long.example"]
    assert draft["migration_review_values"] == {"bot.long.example": 42}
    assert not (configs / "demo_v8").exists()
    assert not list(configs.glob(".migrate-*"))


def test_run_migration_review_uses_run_editor_draft_store(tmp_path, monkeypatch) -> None:
    """V7 Run review output must stay in the PB8 Run editor and never enter Backtest storage."""
    configs, _v7_configs, _queue, _logs = _patch_roots(tmp_path, monkeypatch)
    source = tmp_path / "data" / "run_v7" / "demo" / "config.json"
    source.parent.mkdir(parents=True)
    source.write_text(
        json.dumps(
            {
                "live": {"example": 42},
                "backtest": {"suite": {"enabled": False}},
                "optimize": {"bounds": {"example": [0.0, 1.0]}},
            }
        ),
        encoding="utf-8",
    )
    migrated = {"config_version": "v8.0.0", "backtest": {}, "live": {}, "bot": {}, "optimize": {}}
    monkeypatch.setattr(
        backtest_v8,
        "migrate_pb7_config",
        lambda *_args, **_kwargs: {
            "report": {
                "output_written": True,
                "manual_review_fields": [
                    "live.example",
                    "backtest.suite",
                    "optimize.bounds.example",
                ],
            },
            "config": migrated,
        },
    )

    result = backtest_v8.migrate_v7(
        {
            "source_type": "run_config",
            "source_name": "demo",
            "target_name": "demo_v8",
            "allow_manual_review_output": True,
        },
        session=None,
    )
    from api import v8_instances
    draft = v8_instances.get_v8_editor_draft(result["draft_id"], session=None)

    assert result["editor"] == "run"
    assert "example" not in draft["config"]["live"]
    assert draft["migration_report"]["manual_review_fields"] == ["live.example"]
    assert draft["migration_review_values"] == {"live.example": 42}
    assert not (configs / "demo_v8").exists()


def test_run_legacy_churn_gate_is_canonicalized_by_pb8_without_review(tmp_path, monkeypatch) -> None:
    """Retired Run distance gates must use PB8's canonical automatic migration."""
    configs, _v7_configs, _queue, _logs = _patch_roots(tmp_path, monkeypatch)
    source = tmp_path / "data" / "run_v7" / "demo" / "config.json"
    source.parent.mkdir(parents=True)
    source.write_text(
        json.dumps(
            {
                "live": {"price_distance_threshold": 0.006},
                "backtest": {"suite": {"enabled": False}},
                "optimize": {"bounds": {"example": [0.0, 1.0]}},
            }
        ),
        encoding="utf-8",
    )
    migrated = {
        "config_version": "v8.0.0",
        "live": {
            "order_replacement_churn_gate_activation_count": 10,
            "order_replacement_churn_gate_market_dist_pct": 0.005,
            "order_replacement_churn_gate_stability_minutes": 2.0,
            "order_replacement_churn_gate_window_minutes": 10.0,
        },
        "backtest": {},
        "bot": {},
        "optimize": {},
    }
    captured = {}

    def fake_migrate(source_path, _output_path, **_kwargs):
        migrated_source = json.loads(Path(source_path).read_text(encoding="utf-8"))
        assert "price_distance_threshold" not in migrated_source["live"]
        assert "initial_entry_exec_max_market_dist_pct" not in migrated_source["live"]
        return {
            "report": {"output_written": True, "manual_review_fields": ["backtest.suite"]},
            "config": migrated,
        }

    def fake_prepare(config, **_kwargs):
        captured.update(config["live"])
        prepared = copy.deepcopy(config)
        prepared["live"].pop("initial_entry_exec_max_market_dist_pct")
        prepared["live"]["order_replacement_churn_gate_activation_count"] = 10
        prepared["live"]["order_replacement_churn_gate_market_dist_pct"] = 0.006
        prepared["live"]["order_replacement_churn_gate_stability_minutes"] = 2.0
        prepared["live"]["order_replacement_churn_gate_window_minutes"] = 10.0
        return prepared

    monkeypatch.setattr(backtest_v8, "migrate_pb7_config", fake_migrate)
    monkeypatch.setattr(backtest_v8, "prepare_pb8_config", fake_prepare)

    result = backtest_v8.migrate_v7(
        {
            "source_type": "run_config",
            "source_name": "demo",
            "target_name": "demo_v8",
            "allow_manual_review_output": True,
        },
        session=None,
    )
    from api import v8_instances
    draft = v8_instances.get_v8_editor_draft(result["draft_id"], session=None)

    assert result["review_required"] is False
    assert captured["initial_entry_exec_max_market_dist_pct"] == 0.006
    assert "order_replacement_churn_gate_market_dist_pct" not in captured
    assert draft["config"]["live"]["order_replacement_churn_gate_market_dist_pct"] == 0.006
    assert "initial_entry_exec_max_market_dist_pct" not in draft["config"]["live"]
    assert draft["migration_report"]["manual_review_fields"] == []
    assert draft["migration_report"]["manual_review_required"] is False
    assert draft["migration_report"]["official_review"]["manual_review_fields"] == ["backtest.suite"]
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
    monkeypatch.setattr(backtest_v8, "validate_pb8_override_bundle", lambda *_args: None)
    config = {
        "config_version": "v8.0.0",
        "backtest": {},
        "coin_overrides": {"HYPE": {"override_config_path": "HYPE.json"}},
    }
    sparse = {"bot": {"long": {"risk": {"entry_cooldown_minutes": 3}}}}

    result = backtest_v8.save_config(
        "demo",
        {"config": config, "override_configs": {"HYPE.json": sparse}},
        create_only=True,
        session=None,
    )

    assert result["ok"] is True
    assert json.loads((configs / "demo" / "HYPE.json").read_text(encoding="utf-8")) == sparse
    assert json.loads((configs / "demo" / "backtest.json").read_text(encoding="utf-8"))["coin_overrides"] == config["coin_overrides"]


def test_manual_draft_save_persists_migration_report_with_bundle(tmp_path, monkeypatch) -> None:
    """The migration report should reach disk only when the user explicitly saves the draft."""
    configs, _v7_configs, _queue, _logs = _patch_roots(tmp_path, monkeypatch)
    monkeypatch.setattr(backtest_v8, "prepare_pb8_config", lambda config, **_kwargs: config)
    monkeypatch.setattr(backtest_v8, "cache_prepared_pb8_config", lambda *_args: None)
    monkeypatch.setattr(backtest_v8, "validate_pb8_override_bundle", lambda *_args: None)
    report = {
        "status": "ok_with_adjustments",
        "manual_review_required": False,
        "pbgui_post_migration_adjustments": [{"code": "freeze_disabled_side"}],
    }

    backtest_v8.save_config(
        "converted",
        {
            "config": {"config_version": "v8.0.0", "backtest": {}},
            "override_configs": {},
            "migration_report": report,
        },
        create_only=True,
        inherit_existing_overrides=False,
        session=None,
    )

    assert json.loads((configs / "converted" / "migration_report.json").read_text(encoding="utf-8")) == report


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
    monkeypatch.setattr(backtest_v8, "validate_pb8_override_bundle", lambda _path: None)

    response = backtest_v8.add_to_queue({"name": "demo"}, session=None)

    queue_payload = json.loads((queue / f"{response['filename']}.json").read_text(encoding="utf-8"))
    assert queue_payload["config_snapshot"] == snapshot
    assert queue_payload["exchange"] == ["bybit"]
    assert (queue / "configs" / response["filename"] / "HYPE.json").read_text(encoding="utf-8") == '{"live": {}}'
    assert not (tmp_path / "data" / "bt_v7_queue").exists()


def test_add_named_config_to_queue_normalizes_stale_suite_scenarios(tmp_path, monkeypatch) -> None:
    """Named queue requests must apply the same suite filtering as inline drafts."""
    configs, _v7_configs, queue, _logs = _patch_roots(tmp_path, monkeypatch)
    config_path = configs / "suite" / "backtest.json"
    config_path.parent.mkdir(parents=True)
    config_path.write_text("{}", encoding="utf-8")
    snapshot = {
        "config_version": "v8.0.0",
        "backtest": {
            "exchanges": ["hyperliquid"],
            "suite_enabled": True,
            "scenarios": [
                {"label": "hyperliquid", "exchanges": ["hyperliquid"]},
                {"label": "bybit", "exchanges": ["bybit"]},
            ],
        },
    }
    monkeypatch.setattr(backtest_v8, "load_pb8_config", lambda _path: snapshot)
    monkeypatch.setattr(
        backtest_v8,
        "save_prepared_pb8_config",
        lambda config, path: Path(path).write_text(json.dumps(config), encoding="utf-8") or config,
    )

    response = backtest_v8.add_to_queue({"name": "suite"}, session=None)
    queued = json.loads((queue / f"{response['filename']}.json").read_text(encoding="utf-8"))

    assert queued["config_snapshot"]["backtest"]["scenarios"] == [
        {"label": "hyperliquid", "exchanges": ["hyperliquid"]},
    ]
    assert snapshot["backtest"]["scenarios"][1]["exchanges"] == ["bybit"]


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


def test_add_to_queue_is_idempotent_for_ai_operation_id(tmp_path, monkeypatch) -> None:
    """Approval recovery must not duplicate an already published PB8 backtest job."""
    _configs, _v7_configs, queue, _logs = _patch_roots(tmp_path, monkeypatch)
    config = {"config_version": "v8.0.0", "backtest": {"exchanges": ["bybit"]}}
    monkeypatch.setattr(backtest_v8, "prepare_pb8_config", lambda value, **_kwargs: value)
    monkeypatch.setattr(
        backtest_v8,
        "save_prepared_pb8_config",
        lambda value, path: Path(path).write_text(json.dumps(value), encoding="utf-8") or value,
    )

    first = backtest_v8.add_to_queue(
        {"name": "ai-retest", "config": config, "operation_id": "a" * 32 + "_0_0"}, session=None
    )
    second = backtest_v8.add_to_queue(
        {"name": "ai-retest", "config": config, "operation_id": "a" * 32 + "_0_0"}, session=None
    )

    assert second == {"ok": True, "filename": first["filename"], "idempotent": True}
    assert len(list(queue.glob("*.json"))) == 1


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
        ownership = Path(command[4])
        ownership.parent.mkdir(parents=True, exist_ok=True)
        ownership.write_text(json.dumps({"pid": 4248, "create_time": 123.0}), encoding="utf-8")
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
    monkeypatch.setattr(backtest_v8, "_systemd_user_manager_available", lambda: False)
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


def test_linux_backtest_uses_separate_transient_systemd_unit(tmp_path, monkeypatch) -> None:
    """A PB8 backtest must leave the API service cgroup so API restarts cannot stop it."""
    _configs, _v7_configs, _queue, logs = _patch_roots(tmp_path, monkeypatch)
    calls = []

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(backtest_v8, "_systemd_user_manager_available", lambda: True)
    monkeypatch.setattr(backtest_v8, "which", lambda _name: "/usr/bin/systemd-run")
    monkeypatch.setattr(backtest_v8.subprocess, "run", fake_run)
    log_path = logs / "persistent-job.log"
    log_path.parent.mkdir(parents=True)
    command = ["/venv/bin/python", "/pbgui/pb8_backtest_runner.py", "backtest"]

    process = backtest_v8._launch_backtest_runner("persistent-job", command, Path("/pb8"), log_path)

    assert process is None
    launched, kwargs = calls[0]
    assert launched[:4] == ["/usr/bin/systemd-run", "--user", "--quiet", "--collect"]
    assert any(part.startswith("--unit=pbgui-pb8-backtest-persistent-job-") for part in launched)
    assert "--property=Type=exec" in launched
    assert f"--property=StandardOutput=append:{log_path}" in launched
    assert launched[-3:] == command
    assert kwargs["timeout"] == 15


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


def test_config_list_reports_active_pb8_strategy(tmp_path, monkeypatch) -> None:
    """Saved PB8 backtest summaries should expose canonical live.strategy_kind."""
    configs, _v7_configs, _queue, _logs = _patch_roots(tmp_path, monkeypatch)
    config_dir = configs / "demo"
    config_dir.mkdir(parents=True)
    (config_dir / "backtest.json").write_text(
        json.dumps({
            "backtest": {"exchanges": ["bybit"]},
            "live": {"strategy_kind": "ema_anchor", "approved_coins": {}},
            "bot": {},
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(backtest_v8, "_results_root", lambda: tmp_path / "results")

    rows = backtest_v8.list_configs(session=None)["configs"]

    assert len(rows) == 1
    assert rows[0]["strategy"] == "ema_anchor"


def test_results_are_read_only_from_pb8_root(tmp_path, monkeypatch) -> None:
    """The V8 result parser must not discover analysis files under PB7 roots."""
    pb8_root = tmp_path / "pb8" / "backtests" / "pbgui"
    pb8_analysis = pb8_root / "demo" / "bybit" / "run-1" / "analysis.json"
    pb8_analysis.parent.mkdir(parents=True)
    pb8_analysis.write_text(
        json.dumps(
            {
                "adg_w_usd": 0.02,
                "adg_strategy_eq_w": 0.03,
                "adg": 0.01,
                "gain_usd": 1.25,
                "drawdown_worst_usd": 0.12,
                "drawdown_worst_w_usd": 0.18,
                "sharpe_ratio_usd": 1.8,
                "sharpe_ratio_w_usd": 2.4,
                "sharpe_ratio_strategy_eq_w": 2.8,
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
                "live": {
                    "strategy_kind": "ema_anchor",
                    "approved_coins": {"long": ["BTC"], "short": ["ETH"]},
                },
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
    assert results[0]["adg"] == 0.01
    assert results[0]["adg_usd"] == 0.01
    assert results[0]["adg_w_usd"] == 0.03
    assert results[0]["gain"] == 1.25
    assert results[0]["drawdown_worst"] == 0.12
    assert results[0]["drawdown_worst_w_usd"] == 0.18
    assert results[0]["sharpe_ratio"] == 1.8
    assert results[0]["sharpe_ratio_w_usd"] == 2.8
    assert results[0]["starting_balance"] == 5000
    assert results[0]["final_balance"] == 6250
    assert results[0]["final_equity"] == 6300
    assert results[0]["equity_balance_diff_neg_max"] == 0.04
    assert results[0]["balance_equity_diff"] == 0.04
    assert results[0]["btc_collateral_cap"] == 0.25
    assert results[0]["end_date"] == "2026-07-01"
    assert results[0]["coins_text"] == "BTC, ETH"
    assert results[0]["strategy"] == "ema_anchor"
    assert results[0]["twe_long"] == 2.0
    assert results[0]["pos_long"] == 6


def test_results_support_newest_first_pagination_and_config_filter(tmp_path, monkeypatch) -> None:
    """PB8 result pages should avoid parsing unrelated configs before the first response."""
    root = tmp_path / "pb8" / "backtests" / "pbgui"
    for index, name in enumerate(("older", "newer"), start=1):
        result_dir = root / name / "bybit" / "run-1"
        result_dir.mkdir(parents=True)
        analysis_path = result_dir / "analysis.json"
        analysis_path.write_text(json.dumps({"gain_usd": index}), encoding="utf-8")
        (result_dir / "config.json").write_text(
            json.dumps({"backtest": {"starting_balance": 1000}, "bot": {}, "live": {"approved_coins": {}}}),
            encoding="utf-8",
        )
        os.utime(analysis_path, (index, index))
    monkeypatch.setattr(backtest_v8, "_results_root", lambda: root)
    monkeypatch.setattr(backtest_result_index, "PBGDIR", str(tmp_path))

    first_page = backtest_v8.get_results(offset=0, limit=1, session=None)
    filtered = backtest_v8.get_results(name="older", offset=0, limit=20, session=None)

    assert [item["config_name"] for item in first_page["results"]] == ["newer"]
    assert first_page["pagination"] == {
        "total": 2,
        "offset": 0,
        "limit": 1,
        "returned": 1,
        "has_more": True,
        "next_offset": 1,
    }
    assert [item["config_name"] for item in filtered["results"]] == ["older"]
    assert filtered["pagination"]["has_more"] is False


def test_results_reuse_persistent_summary_index(tmp_path, monkeypatch) -> None:
    """A warm PB8 result request must not parse unchanged analysis and config files again."""
    root = tmp_path / "pb8" / "backtests" / "pbgui"
    result_dir = root / "demo" / "bybit" / "run-1"
    result_dir.mkdir(parents=True)
    (result_dir / "analysis.json").write_text(json.dumps({"gain_usd": 1.2}), encoding="utf-8")
    (result_dir / "config.json").write_text(
        json.dumps({"backtest": {"starting_balance": 1000}, "bot": {}, "live": {}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(backtest_v8, "_results_root", lambda: root)
    monkeypatch.setattr(backtest_result_index, "PBGDIR", str(tmp_path))

    first = backtest_v8.get_results(offset=0, limit=0, session=None)
    monkeypatch.setattr(
        backtest_v8,
        "_list_results",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("warm cache rebuilt")),
    )
    second = backtest_v8.get_results(offset=0, limit=0, session=None)

    assert second == first


def test_result_list_uses_analysis_terminal_values_without_opening_large_csv(tmp_path, monkeypatch) -> None:
    """Modern compact summaries must not stream balance history during list loading."""
    root = tmp_path / "pb8" / "backtests" / "pbgui"
    result_dir = root / "demo" / "bybit" / "run-1"
    result_dir.mkdir(parents=True)
    (result_dir / "analysis.json").write_text(
        json.dumps(
            {
                "starting_balance_usd": 1000,
                "final_balance_usd": 1250,
                "final_equity_usd": 1240,
                "gain_usd": 1.25,
            }
        ),
        encoding="utf-8",
    )
    (result_dir / "config.json").write_text(
        json.dumps({"backtest": {"starting_balance": 1000}, "live": {"approved_coins": {}}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(backtest_v8, "_results_root", lambda: root)
    monkeypatch.setattr(
        backtest_v8,
        "_result_terminal_balances",
        lambda _path: (_ for _ in ()).throw(AssertionError("large balance CSV opened")),
    )

    result = backtest_v8._list_results()[0]

    assert result["final_balance"] == 1250
    assert result["final_equity"] == 1240


def test_combined_results_report_configured_exchanges(tmp_path, monkeypatch) -> None:
    """Combined PB8 result directories must retain the real exchanges needed by chart controls."""
    root = tmp_path / "pb8" / "backtests" / "pbgui"
    result_dir = root / "demo" / "combined" / "run-1"
    result_dir.mkdir(parents=True)
    (result_dir / "analysis.json").write_text(json.dumps({"gain_usd": 1.2}), encoding="utf-8")
    (result_dir / "config.json").write_text(
        json.dumps(
            {
                "backtest": {"exchanges": ["binance", "bybit"], "starting_balance": 1000},
                "live": {"approved_coins": {"long": ["HYPE"], "short": []}},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(backtest_v8, "_results_root", lambda: root)

    results = backtest_v8._list_results()

    assert results[0]["exchange_dir"] == "combined"
    assert results[0]["exchanges"] == ["binance", "bybit"]
    assert results[0]["coins"] == ["HYPE"]


def test_terminal_balance_reader_uses_last_values_from_gzip_csv(tmp_path, monkeypatch) -> None:
    """Lazy PB8 detail reads retain authoritative compressed CSV support."""
    root = tmp_path / "pb8-results"
    result_dir = root / "demo" / "bybit" / "run-1"
    result_dir.mkdir(parents=True)
    (result_dir / "analysis.json").write_text(json.dumps({"gain_usd": 9.0}), encoding="utf-8")
    (result_dir / "config.json").write_text(json.dumps({"backtest": {"starting_balance": 1000}}), encoding="utf-8")
    with gzip.open(result_dir / "balance_and_equity.csv.gz", "wt", encoding="utf-8", newline="") as handle:
        handle.write("minute,usd_total_balance,usd_total_equity\n0,1000,990\n1,1234.5,1201.25\n")
    result = backtest_v8._result_terminal_balances(result_dir)

    assert result["usd_total_balance"] == 1234.5
    assert result["usd_total_equity"] == 1201.25


def test_results_fall_back_to_gzip_when_plain_terminal_csv_is_invalid(tmp_path, monkeypatch) -> None:
    """A corrupt preferred CSV must not hide a valid compressed PB8 terminal artifact."""
    root = tmp_path / "pb8-results"
    result_dir = root / "demo" / "bybit" / "run-1"
    result_dir.mkdir(parents=True)
    (result_dir / "analysis.json").write_text("{}", encoding="utf-8")
    (result_dir / "balance_and_equity.csv").write_bytes(b"\xff\xfeinvalid")
    with gzip.open(result_dir / "balance_and_equity.csv.gz", "wt", encoding="utf-8", newline="") as handle:
        handle.write("usd_total_balance,usd_total_equity\n1500,1400\n")
    monkeypatch.setattr(backtest_v8, "_log", lambda *_args, **_kwargs: None)

    result = backtest_v8._result_terminal_balances(result_dir)

    assert result["usd_total_balance"] == 1500
    assert result["usd_total_equity"] == 1400


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
    result = backtest_v8._result_terminal_balances(result_dir)

    assert result["usd_total_balance"] == 1500
    assert result["usd_total_equity"] == 1400


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
    monkeypatch.setattr(backtest_v8, "get_pb8_coin_override_metadata", lambda mode, strategy: {
        "contract_version": 1,
        "hsl_signal_mode": mode,
        "strategy_kind": strategy,
        "params": {
            "bot": {"long": {"hsl.enabled": {"type": "boolean", "default": False}, "hsl.restart_after_red_policy": {"type": "string", "default": "threshold"}}, "short": {}},
            "live": {"leverage": {"type": "number", "default": 10}},
        },
    })

    params = backtest_v8.get_override_params("coin", "trailing_martingale", session=None)["params"]

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
        ownership = Path(command[4])
        ownership.parent.mkdir(parents=True, exist_ok=True)
        ownership.write_text(json.dumps({"pid": 4250, "create_time": 123.0}), encoding="utf-8")
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
    monkeypatch.setattr(backtest_v8, "_systemd_user_manager_available", lambda: False)
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
    monkeypatch.setattr(backtest_v8, "get_pb8_exchange_metadata", lambda: {"backtest": ["binance", "weex"]})

    settings = backtest_v8.get_settings(None)
    assert settings == {
        "autostart": True,
        "cpu": 8,
        "cpu_max": 16,
        "use_pbgui_market_data": True,
        "hsl_signal_modes": ["coin", "pside", "unified"],
        "exchange_options": ["binance", "weex"],
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
