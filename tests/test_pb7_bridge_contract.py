"""Contract tests for PB7 metadata, archive layout, and PB7 runtime preflights."""

from __future__ import annotations

import json
import asyncio
import subprocess
import datetime
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from api import archive_helpers
from api import backtest_v7
from api.archive_helpers import (
    ARCHIVE_MANIFEST,
    ARCHIVE_REPORT,
    load_archive_manifest,
    load_archive_readme_config,
    build_archive_score_payload,
    build_archive_scores_markdown,
    copy_backtest_result_to_archive,
    save_archive_readme_config,
    derive_backtest_archive_relative_path,
    derive_optimize_archive_relative_path,
    ensure_config_version,
    migrate_archive_layout,
    remove_duplicate_results,
    remove_liquidated_results,
    score_archive_results,
    update_archive_readme,
    update_archive_scores_and_readme,
)
from api import pb7_bridge
from pbgui_purefunc import pb7_suite_preflight_errors


ROOT = Path(__file__).resolve().parents[1]


def build_hsl_preflight_config() -> dict:
    """Build a minimal optimize config for HSL preflight tests."""
    return {
        "backtest": {},
        "bot": {
            "long": {
                "hsl_enabled": True,
                "hsl_red_threshold": 0.2,
            },
            "short": {
                "hsl_enabled": False,
                "hsl_red_threshold": 0.2,
            },
        },
        "optimize": {
            "bounds": {},
            "fixed_params": [],
            "fixed_runtime_overrides": {},
        },
    }


def write_archive_json(path: Path, payload: dict) -> None:
    """Write a small archive JSON fixture file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=4), encoding="utf-8")


def write_archive_result(root: Path, *, version: str | None = "v7.4.2", liquidated: bool = False) -> Path:
    """Create a minimal backtest result fixture and return its directory."""
    result_dir = root / "my_config" / "bybit" / "2026-06-21T120000Z"
    config = {
        "backtest": {
            "base_dir": "backtests/pbgui/my_config",
            "exchanges": ["bybit"],
            "starting_balance": 1000,
        },
        "bot": {"long": {}, "short": {}},
        "pbgui": {"version": "999"},
    }
    if version is not None:
        config["config_version"] = version
    write_archive_json(result_dir / "config.json", config)
    write_archive_json(result_dir / "analysis.json", {"gain": 1.2, "liquidated": liquidated})
    return result_dir


def init_clean_archive_git_repo(path: Path) -> None:
    """Initialize and commit a clean temporary archive git repository."""
    subprocess.run(["git", "init"], cwd=str(path), check=True, capture_output=True, text=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=str(path), check=True, capture_output=True, text=True)
    subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=str(path), check=True, capture_output=True, text=True)
    subprocess.run(["git", "add", "-A"], cwd=str(path), check=True, capture_output=True, text=True)
    subprocess.run(["git", "commit", "-m", "initial"], cwd=str(path), check=True, capture_output=True, text=True)


def test_optimize_metric_sets_come_from_pb7_metrics(monkeypatch) -> None:
    """Metric set helper should centralize PB7 config.metrics access."""
    metrics_module = SimpleNamespace(
        CURRENCY_METRICS={"gain", "adg_w"},
        SHARED_METRICS={"positions_held_per_day"},
        ANALYSIS_SHARED_KEYS={"total_wallet_exposure_mean"},
    )

    def fake_import(module_name: str):
        """Return the fake PB7 module requested by the bridge."""
        assert module_name == "config.metrics"
        return metrics_module

    monkeypatch.setattr(pb7_bridge, "_import_pb7_module", fake_import)

    payload = pb7_bridge.get_optimize_metric_sets()

    assert payload["currency_metrics"] == ["adg_w", "gain"]
    assert "positions_held_per_day" in payload["shared_metrics"]
    assert "total_wallet_exposure_mean" in payload["shared_metrics"]
    assert "backtest_completion_ratio" in payload["shared_metrics"]


def test_optimize_limits_meta_uses_metric_sets(monkeypatch) -> None:
    """Limits metadata should be buildable from PB7 metrics/limits/scoring modules."""
    modules = {
        "config.metrics": SimpleNamespace(
            CURRENCY_METRICS={"gain"},
            SHARED_METRICS={"loss_profit_ratio"},
            ANALYSIS_SHARED_KEYS={"total_wallet_exposure_mean"},
        ),
        "config.limits": SimpleNamespace(SUPPORTED_LIMIT_STATS={"min", "max"}),
        "config.scoring": SimpleNamespace(
            OBJECTIVE_GOALS=("min", "max"),
            default_objective_goal=lambda metric: "max" if metric != "loss_profit_ratio" else "min",
        ),
    }

    def fake_import(module_name: str):
        """Return a fake PB7 module by import name."""
        return modules[module_name]

    monkeypatch.setattr(pb7_bridge, "_import_pb7_module", fake_import)

    payload = pb7_bridge.get_optimize_limits_meta_payload()

    assert "gain_usd" in payload["all_valid_metrics"]
    assert "total_wallet_exposure_mean" in payload["all_valid_metrics"]
    assert payload["default_goal_map"]["gain_usd"] == "max"
    assert "max" in payload["stat_options"]


def test_optimize_scoring_default_goals_use_pb7_scoring(monkeypatch) -> None:
    """Scoring goal helper should centralize PB7 default goal lookups."""
    scoring_module = SimpleNamespace(default_objective_goal=lambda metric: "min" if "drawdown" in metric else "max")

    def fake_import(module_name: str):
        """Return the fake PB7 scoring module requested by the bridge."""
        assert module_name == "config.scoring"
        return scoring_module

    monkeypatch.setattr(pb7_bridge, "_import_pb7_module", fake_import)

    payload = pb7_bridge.get_optimize_scoring_default_goals(["gain_usd", "drawdown_worst_usd"])

    assert payload == {"gain_usd": "max", "drawdown_worst_usd": "min"}


def test_pb7_preflight_rejects_active_hsl_red_threshold_bound_at_zero() -> None:
    """Active HSL must not allow optimizer candidates with red_threshold=0."""
    cfg = build_hsl_preflight_config()
    cfg["optimize"]["bounds"]["long_hsl_red_threshold"] = [0, 1, 0.001]

    errors = pb7_suite_preflight_errors(cfg)

    assert "optimize.bounds.long_hsl_red_threshold lower bound must be > 0.0" in errors[0]


def test_pb7_preflight_allows_positive_active_hsl_red_threshold_bound() -> None:
    """Positive HSL red-threshold bounds are valid for active HSL optimization."""
    cfg = build_hsl_preflight_config()
    cfg["optimize"]["bounds"]["long_hsl_red_threshold"] = [0.001, 1, 0.001]

    assert pb7_suite_preflight_errors(cfg) == []


def test_pb7_preflight_allows_zero_hsl_bound_when_hsl_disabled() -> None:
    """Disabled HSL may keep neutral red-threshold bounds at zero."""
    cfg = build_hsl_preflight_config()
    cfg["bot"]["long"]["hsl_enabled"] = False
    cfg["optimize"]["bounds"]["long_hsl_red_threshold"] = [0, 1, 0.001]

    assert pb7_suite_preflight_errors(cfg) == []


def test_pb7_preflight_uses_runtime_hsl_enabled_override() -> None:
    """A runtime override enabling HSL makes the red-threshold bound safety check active."""
    cfg = build_hsl_preflight_config()
    cfg["bot"]["long"]["hsl_enabled"] = False
    cfg["optimize"]["fixed_runtime_overrides"] = {"bot.long.hsl_enabled": True}
    cfg["optimize"]["bounds"]["long_hsl_red_threshold"] = [0, 1, 0.001]

    errors = pb7_suite_preflight_errors(cfg)

    assert "optimize.bounds.long_hsl_red_threshold lower bound must be > 0.0" in errors[0]


def test_optimize_editor_missing_hsl_override_falls_back_to_visible_bot_value() -> None:
    """Saving must not disable HSL when an absent override displays as enabled."""
    source = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    start = source.index("function collectOptimizeRuntimeOverrides(")
    end = source.index("function toNullableNumber(", start)
    collector = source[start:end]

    assert "result[field.key] = getOptimizeConfigBotHslEnabled(field.side);" in collector
    assert "result[field.key] = !!field.defaultValue;" not in collector


def test_optimize_websocket_reconnect_retires_stale_socket() -> None:
    """Optimize reconnects must not let a replaced socket schedule another reconnect."""
    source = (ROOT / "frontend" / "v7_optimize.html").read_text(encoding="utf-8")
    start = source.index("function connectWS(")
    end = source.index("function handleError(", start)
    connect_source = source[start:end]

    assert "wsReconnectTimer: null" in source
    assert "state.ws.onopen = state.ws.onmessage = state.ws.onclose = state.ws.onerror = null;" in connect_source
    assert connect_source.count("if (state.ws !== socket) return;") == 3
    assert "state.ws = null;" in connect_source
    assert "if (!state.wsReconnectTimer)" in connect_source
    assert "if (state.wsReconnectTimer !== reconnectTimer) return;" in connect_source
    assert "state.wsReconnectTimer = null;\n        connectWS();" in connect_source


def test_pb7_preflight_allows_fixed_zero_bound_with_positive_active_hsl_config() -> None:
    """A fixed red-threshold bound is safe when PB7 will use the positive current config value."""
    cfg = build_hsl_preflight_config()
    cfg["optimize"]["bounds"]["long_hsl_red_threshold"] = [0, 1, 0.001]
    cfg["optimize"]["fixed_params"] = ["long_hsl_red_threshold"]

    assert pb7_suite_preflight_errors(cfg) == []


def test_pb7_preflight_rejects_nonpositive_active_hsl_base_threshold_even_if_fixed() -> None:
    """A fixed bound cannot rescue an active HSL config whose base threshold is zero."""
    cfg = build_hsl_preflight_config()
    cfg["bot"]["long"]["hsl_red_threshold"] = 0
    cfg["optimize"]["bounds"]["long_hsl_red_threshold"] = [0, 1, 0.001]
    cfg["optimize"]["fixed_params"] = ["long_hsl_red_threshold"]

    errors = pb7_suite_preflight_errors(cfg)

    assert "Long HSL red threshold must be > 0.0" in errors[0]


def test_pb7_preflight_keeps_hsl_errors_when_suite_includes_base_scenario() -> None:
    """Suite include-base configs still need optimize HSL bound validation."""
    cfg = build_hsl_preflight_config()
    cfg["backtest"]["suite"] = {
        "enabled": True,
        "include_base_scenario": True,
        "scenarios": [{"name": "base"}],
    }
    cfg["optimize"]["bounds"]["long_hsl_red_threshold"] = [0, 1, 0.001]

    errors = pb7_suite_preflight_errors(cfg)

    assert "optimize.bounds.long_hsl_red_threshold lower bound must be > 0.0" in errors[0]


def test_backtest_archive_path_uses_pb7_config_version_not_pbgui_version(tmp_path: Path) -> None:
    """Backtest archive paths are derived from config_version, not pbgui.version."""
    archive_root = tmp_path / "archive"
    result_dir = write_archive_result(tmp_path / "results")

    rel_path, meta = derive_backtest_archive_relative_path(result_dir, archive_root)

    assert rel_path == Path("pbgui/configs/v7.4.2/backtests/my_config/bybit/2026-06-21T120000Z")
    assert meta["pb7_config_version"] == "v7.4.2"
    assert meta["pbgui_version"] == "999"


def test_add_config_to_archive_accepts_missing_dest_name(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The legacy add-config route no longer requires a frontend dest_name."""
    archive_root = tmp_path / "archives"
    (archive_root / "mine").mkdir(parents=True)
    result_dir = write_archive_result(tmp_path / "results")
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archive_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")
    monkeypatch.setattr(backtest_v7, "_resolve_result_dir", lambda path: Path(path).resolve())

    response = asyncio.run(backtest_v7.add_config_to_archive("mine", {"source_path": str(result_dir)}, session=None))

    assert response["ok"] is True
    assert response["relative_path"] == "pbgui/configs/v7.4.2/backtests/my_config/bybit/2026-06-21T120000Z"
    assert (Path(response["path"]) / "analysis.json").exists()
    assert response["manifest"]["schema_version"] == 1
    assert any(item["path"] == response["relative_path"] for item in response["manifest"]["items"])


def test_unknown_config_version_adds_result_fingerprint_suffix(tmp_path: Path) -> None:
    """Backtests without PB7 config_version are placed under unknown with a suffix."""
    archive_root = tmp_path / "archive"
    result_dir = write_archive_result(tmp_path / "results", version=None)

    copied = copy_backtest_result_to_archive(result_dir, archive_root)

    assert copied["relative_path"].startswith("pbgui/configs/unknown/backtests/my_config/bybit/2026-06-21T120000Z__")
    assert (Path(copied["path"]) / "config.json").exists()


def test_migration_moves_legacy_results_and_writes_report(tmp_path: Path) -> None:
    """Migration moves clean legacy results into the versioned layout."""
    archive_root = tmp_path / "archive"
    legacy_result = write_archive_result(archive_root / "legacy_root")
    init_clean_archive_git_repo(archive_root)

    report = migrate_archive_layout(archive_root)

    target = archive_root / "pbgui/configs/v7.4.2/backtests/my_config/bybit/2026-06-21T120000Z"
    assert report["migrated"] == 1
    assert not legacy_result.exists()
    assert (target / "analysis.json").exists()
    assert (archive_root / ARCHIVE_REPORT).exists()
    manifest = load_archive_manifest(archive_root)
    assert manifest is not None
    assert (archive_root / ARCHIVE_MANIFEST).exists()
    assert any(item["type"] == "backtest_result" for item in manifest["items"])


def test_migration_skips_dirty_git_worktree(tmp_path: Path) -> None:
    """Migration refuses dirty archive clones."""
    archive_root = tmp_path / "archive"
    legacy_result = write_archive_result(archive_root / "legacy_root")
    init_clean_archive_git_repo(archive_root)
    (archive_root / "dirty.txt").write_text("dirty", encoding="utf-8")

    report = migrate_archive_layout(archive_root)

    assert report["skipped"] is True
    assert report["reason"] == "dirty_worktree"
    assert legacy_result.exists()


def test_migration_status_fast_avoids_legacy_scan_after_migration_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dirty migrated archives should not rescan every result just to render status."""
    archive_root = tmp_path / "archive"
    write_archive_json(archive_root / ARCHIVE_REPORT, {"migrated": 3})
    monkeypatch.setattr(
        archive_helpers,
        "git_worktree_state",
        lambda root: {"is_git": True, "dirty": True, "porcelain": "M pbgui/archive_manifest.json"},
    )

    def fail_legacy_scan(root: Path) -> list[Path]:
        """Fail if archive_migration_status falls back to the expensive scan."""
        raise AssertionError("legacy_result_dirs should not be called")

    monkeypatch.setattr(archive_helpers, "legacy_result_dirs", fail_legacy_scan)

    status = archive_helpers.archive_migration_status(archive_root)

    assert status["status"] == "migrated_pending_push"
    assert status["legacy_count"] == 0


def test_archive_result_listing_does_not_fingerprint_result_dirs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Archive result listing must avoid expensive per-result directory fingerprints."""
    archive_root = tmp_path / "archive"
    write_archive_result(archive_root / "pbgui/configs/v7.4.2/backtests")

    def fail_directory_fingerprint(path: Path) -> str:
        """Fail if summary listing falls back to the expensive directory hash."""
        raise AssertionError("directory_fingerprint should not be called while listing results")

    monkeypatch.setattr(archive_helpers, "directory_fingerprint", fail_directory_fingerprint)

    results = archive_helpers.list_archive_backtest_results(archive_root)

    assert len(results) == 1
    assert results[0]["pb7_config_version"] == "v7.4.2"


def test_archive_result_listing_includes_dataset_coins(tmp_path: Path) -> None:
    """Archive result summaries expose coins from PB7 dataset metadata."""
    archive_root = tmp_path / "archive"
    result_dir = write_archive_result(archive_root / "pbgui/configs/v7.4.2/backtests")
    write_archive_json(result_dir / "dataset.json", {"coins": ["HYPE", "hype", "BTC"]})

    results = archive_helpers.list_archive_backtest_results(archive_root)

    assert len(results) == 1
    assert results[0]["coins"] == ["HYPE", "BTC"]
    assert results[0]["coins_text"] == "HYPE, BTC"


def test_optimize_archive_path_uses_pb7_config_version() -> None:
    """Optimize archive paths also use PB7 config_version instead of pbgui.version."""
    rel_path, meta = derive_optimize_archive_relative_path(
        "optimizer_a",
        {"config_version": "v7.4.2", "pbgui": {"version": "1000"}, "optimize": {}},
    )

    assert rel_path == Path("pbgui/configs/v7.4.2/optimize/optimizer_a.json")
    assert meta["pb7_config_version"] == "v7.4.2"
    assert meta["pbgui_version"] == "1000"


def test_ensure_config_version_injects_template_version() -> None:
    """Optimize saves can inject the current PB7 template config_version."""
    config = {"optimize": {}}

    result = ensure_config_version(config, lambda: {"config_version": "v7.4.2"})

    assert result["config_version"] == "v7.4.2"


def test_remove_liquidated_dry_run_reports_without_deleting(tmp_path: Path) -> None:
    """Liquidated cleanup dry-run reports matches without removing files."""
    archive_root = tmp_path / "archive"
    liquidated = write_archive_result(archive_root / "pbgui/configs/v7.4.2/backtests", liquidated=True)
    healthy = write_archive_result(archive_root / "other", liquidated=False)

    response = remove_liquidated_results(archive_root, [str(liquidated), str(healthy)], "selected_results", True)

    assert response["matched"] == 1
    assert response["removed"] == 0
    assert liquidated.exists()
    assert healthy.exists()


def test_remove_duplicate_results_keeps_newest_copy(tmp_path: Path) -> None:
    """Duplicate cleanup removes older exact duplicate archive results and keeps unique results."""
    archive_root = tmp_path / "archive"
    base = archive_root / "pbgui/configs/v7.12.0/backtests/my_config/combined"

    def make_result(name: str, run_id: str, gain: float = 1.2, runtime: int = 1) -> Path:
        result_dir = base / name
        config = {
            "config_version": "v7.12.0",
            "backtest": {"base_dir": "backtests/pbgui/my_config", "exchanges": ["binance", "bybit"], "starting_balance": 1000},
            "bot": {"long": {"total_wallet_exposure_limit": 2.0}, "short": {"total_wallet_exposure_limit": 0.0}},
            "pbgui": {"archive_retest": {"run_id": run_id}},
        }
        analysis = {"adg": 0.001, "gain": gain, "drawdown_worst": 0.2, "sharpe_ratio": 0.5, "runtime_seconds": runtime}
        write_archive_json(result_dir / "config.json", config)
        write_archive_json(result_dir / "analysis.json", analysis)
        return result_dir

    older = make_result("2026-06-22T010000Z", "old-run", runtime=10)
    newest = make_result("2026-06-22T020000Z", "new-run", runtime=20)
    unique = make_result("2026-06-22T030000Z", "unique-run", gain=1.3)
    paths = [str(older), str(newest), str(unique)]

    preview = remove_duplicate_results(archive_root, paths, "selected_results", True)

    assert preview["matched"] == 1
    assert preview["removed"] == 0
    assert preview["items"][0]["path"] == str(older)
    assert preview["items"][0]["keep_path"] == str(newest)
    assert older.exists()

    result = remove_duplicate_results(archive_root, paths, "selected_results", False)

    assert result["removed"] == 1
    assert not older.exists()
    assert newest.exists()
    assert unique.exists()


def test_remove_liquidated_route_refuses_non_own_archive(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The cleanup API only mutates the configured own archive."""
    archives_root = tmp_path / "archives"
    (archives_root / "other").mkdir(parents=True)
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")

    with pytest.raises(HTTPException) as exc_info:
        backtest_v7.remove_archive_liquidated_results("other", {"paths": [], "dry_run": True}, session=None)

    assert exc_info.value.status_code == 403


def test_rename_archive_backtest_config_moves_group_and_rewrites_base_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Own archive backtest config rename moves the group and updates result configs."""
    archives_root = tmp_path / "archives"
    archive_root = archives_root / "mine"
    result_dir = write_archive_result(archive_root / "pbgui/configs/v7.4.2/backtests")
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")

    response = backtest_v7.rename_archive_backtest_config(
        "mine",
        {"path": str(result_dir), "new_name": "HYPE"},
        session=None,
    )

    target_result = archive_root / "pbgui/configs/v7.4.2/backtests/HYPE/bybit/2026-06-21T120000Z"
    assert response["ok"] is True
    assert response["changed"] is True
    assert response["old_name"] == "my_config"
    assert response["new_name"] == "HYPE"
    assert Path(response["path"]) == target_result
    assert not (archive_root / "pbgui/configs/v7.4.2/backtests/my_config").exists()
    assert (target_result / "analysis.json").exists()
    config = json.loads((target_result / "config.json").read_text(encoding="utf-8"))
    assert config["backtest"]["base_dir"] == "backtests/pbgui/HYPE"
    assert (archive_root / ARCHIVE_MANIFEST).exists()


def test_rename_archive_backtest_config_refuses_foreign_archive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Backtest config rename is restricted to the configured own archive."""
    archives_root = tmp_path / "archives"
    archive_root = archives_root / "other"
    result_dir = write_archive_result(archive_root / "pbgui/configs/v7.4.2/backtests")
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")

    with pytest.raises(HTTPException) as exc_info:
        backtest_v7.rename_archive_backtest_config("other", {"path": str(result_dir), "new_name": "HYPE"}, session=None)

    assert exc_info.value.status_code == 403


def test_rename_archive_backtest_config_refuses_collision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Backtest config rename must not overwrite an existing archive config group."""
    archives_root = tmp_path / "archives"
    archive_root = archives_root / "mine"
    result_dir = write_archive_result(archive_root / "pbgui/configs/v7.4.2/backtests")
    (archive_root / "pbgui/configs/v7.4.2/backtests/HYPE").mkdir(parents=True)
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")

    with pytest.raises(HTTPException) as exc_info:
        backtest_v7.rename_archive_backtest_config("mine", {"path": str(result_dir), "new_name": "HYPE"}, session=None)

    assert exc_info.value.status_code == 409
    assert (archive_root / "pbgui/configs/v7.4.2/backtests/my_config").exists()


def test_rename_archive_backtest_config_rejects_malformed_config_without_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed required config JSON aborts before moving or rewriting the group."""
    archives_root = tmp_path / "archives"
    archive_root = archives_root / "mine"
    result_dir = write_archive_result(archive_root / "pbgui/configs/v7.4.2/backtests")
    config_file = result_dir / "config.json"
    config_file.write_text("{not-json", encoding="utf-8")
    original_bytes = config_file.read_bytes()
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")

    with pytest.raises(HTTPException) as exc_info:
        backtest_v7.rename_archive_backtest_config(
            "mine", {"path": str(result_dir), "new_name": "HYPE"}, session=None
        )

    assert exc_info.value.status_code == 422
    assert config_file.read_bytes() == original_bytes
    assert result_dir.exists()
    assert not (archive_root / "pbgui/configs/v7.4.2/backtests/HYPE").exists()


def test_rename_archive_backtest_config_requires_config_for_every_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every analysis result must have a regular JSON-object config before group staging."""
    archives_root = tmp_path / "archives"
    archive_root = archives_root / "mine"
    result_dir = write_archive_result(archive_root / "pbgui/configs/v7.4.2/backtests")
    (result_dir / "config.json").unlink()
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")

    with pytest.raises(HTTPException) as exc_info:
        backtest_v7.rename_archive_backtest_config(
            "mine", {"path": str(result_dir), "new_name": "HYPE"}, session=None
        )

    assert exc_info.value.status_code == 422
    assert result_dir.exists()
    assert not (archive_root / "pbgui/configs/v7.4.2/backtests/HYPE").exists()


def test_rename_archive_backtest_config_rejects_predictable_tmp_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A legacy config.json.tmp symlink aborts rename without touching its target."""
    archives_root = tmp_path / "archives"
    archive_root = archives_root / "mine"
    result_dir = write_archive_result(archive_root / "pbgui/configs/v7.4.2/backtests")
    outside = tmp_path / "outside.json"
    outside.write_text("outside", encoding="utf-8")
    (result_dir / "config.json.tmp").symlink_to(outside)
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")

    with pytest.raises(HTTPException) as exc_info:
        backtest_v7.rename_archive_backtest_config(
            "mine", {"path": str(result_dir), "new_name": "HYPE"}, session=None
        )

    assert exc_info.value.status_code == 422
    assert outside.read_text(encoding="utf-8") == "outside"
    assert result_dir.exists()


def test_rename_archive_backtest_config_restores_original_after_install_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failure after staged installation restores the untouched original group."""
    archives_root = tmp_path / "archives"
    archive_root = archives_root / "mine"
    result_dir = write_archive_result(archive_root / "pbgui/configs/v7.4.2/backtests")
    original_config = (result_dir / "config.json").read_bytes()
    real_write = backtest_v7.write_archive_json
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")

    def fail_installed_write(path: Path, payload: dict, archive_root_arg: Path) -> None:
        """Fail only after the staged group has entered the Git archive."""
        if archive_root_arg == archive_root:
            raise OSError("installed write failed")
        real_write(path, payload, archive_root_arg)

    monkeypatch.setattr(backtest_v7, "write_archive_json", fail_installed_write)

    with pytest.raises(HTTPException, match="installed write failed"):
        backtest_v7.rename_archive_backtest_config(
            "mine", {"path": str(result_dir), "new_name": "HYPE"}, session=None
        )

    assert result_dir.exists()
    assert (result_dir / "config.json").read_bytes() == original_config
    assert not (archive_root / "pbgui/configs/v7.4.2/backtests/HYPE").exists()
    assert not list(archives_root.glob(".pbgui-archive-rename-stage-*"))
    assert not list(archives_root.glob(".pbgui-archive-rename-backup-*"))


def test_delete_archive_optimize_config_removes_config_and_meta(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Deleting an archived Optimize config removes its JSON and sidecar metadata."""
    archives_root = tmp_path / "archives"
    archive_root = archives_root / "mine"
    config_file = archive_root / "pbgui/configs/v7.4.2/optimize/optimizer_a.json"
    meta_file = config_file.with_name("optimizer_a.meta.json")
    write_archive_json(config_file, {"config_version": "v7.4.2", "optimize": {}})
    write_archive_json(meta_file, {"name": "optimizer_a"})
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")

    response = backtest_v7.delete_archive_optimize_config("mine", str(config_file), session=None)

    assert response["ok"] is True
    assert response["relative_path"] == "pbgui/configs/v7.4.2/optimize/optimizer_a.json"
    assert not config_file.exists()
    assert not meta_file.exists()
    assert load_archive_manifest(archive_root)["items"] == []


def test_delete_archive_optimize_config_refuses_non_own_archive(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Archived Optimize config deletion is restricted to the configured own archive."""
    archives_root = tmp_path / "archives"
    config_file = archives_root / "other/pbgui/configs/v7.4.2/optimize/optimizer_a.json"
    write_archive_json(config_file, {"config_version": "v7.4.2", "optimize": {}})
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")

    with pytest.raises(HTTPException) as exc_info:
        backtest_v7.delete_archive_optimize_config("other", str(config_file), session=None)

    assert exc_info.value.status_code == 403
    assert config_file.exists()


def test_delete_archive_optimize_config_rejects_path_outside_archive(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Archived Optimize config deletion rejects path traversal outside the archive root."""
    archives_root = tmp_path / "archives"
    (archives_root / "mine").mkdir(parents=True)
    outside_file = tmp_path / "outside.json"
    write_archive_json(outside_file, {"optimize": {}})
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")

    with pytest.raises(HTTPException) as exc_info:
        backtest_v7.delete_archive_optimize_config("mine", str(outside_file), session=None)

    assert exc_info.value.status_code == 400
    assert outside_file.exists()


def test_archive_readme_config_writes_static_section_and_preserves_scores(tmp_path: Path) -> None:
    """Archive README config writes static Markdown without overwriting generated scores."""
    archive_root = tmp_path / "archive"
    archive_root.mkdir()
    config = save_archive_readme_config(archive_root, {
        "title": "Mani Archive",
        "static_markdown": "Static notes for GitHub.",
    })

    update_archive_readme(archive_root, config, scores_markdown="| Score | Result |\n|---:|---|\n| 9.1 | BTC |")
    changed = save_archive_readme_config(archive_root, {
        "title": "Mani Archive",
        "static_markdown": "Updated static notes.",
    })
    update_archive_readme(archive_root, changed)

    readme = (archive_root / "README.md").read_text(encoding="utf-8")
    loaded = load_archive_readme_config(archive_root)
    assert loaded["title"] == "Mani Archive"
    assert loaded["static_markdown"] == "Updated static notes."
    assert "# Mani Archive" in readme
    assert "Updated static notes." in readme
    assert "| 9.1 | BTC |" in readme


def test_archive_readme_settings_route_refuses_non_own_archive(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """README config writes are restricted to the configured own archive."""
    archives_root = tmp_path / "archives"
    (archives_root / "other").mkdir(parents=True)
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")

    with pytest.raises(HTTPException) as exc_info:
        backtest_v7.save_archive_readme_settings("other", {"title": "Other"}, session=None)

    assert exc_info.value.status_code == 403


def test_archive_scores_rank_good_results_above_risky_results() -> None:
    """PBGui archive scores combine return, risk, ratios, curve quality, and data quality."""
    good = {
        "config_name": "good",
        "result_name": "2026-06-21T120000Z",
        "exchange_dir": "bybit",
        "display_name": "good/result",
        "start_date": "2025-01-01",
        "end_date": "2026-01-01",
        "adg": 0.004,
        "gain": 3.0,
        "drawdown_worst": 0.12,
        "sharpe_ratio": 1.8,
        "analysis": {
            "adg": 0.004,
            "gain": 3.0,
            "drawdown_worst": 0.12,
            "sharpe_ratio": 1.8,
            "sortino_ratio": 2.2,
            "omega_ratio": 1.35,
            "equity_choppiness": 0.2,
            "equity_jerkiness": 0.15,
            "backtest_completion_ratio": 1.0,
        },
    }
    risky = {
        "config_name": "risky",
        "result_name": "2026-06-21T120000Z",
        "exchange_dir": "bybit",
        "display_name": "risky/result",
        "start_date": "2025-01-01",
        "end_date": "2026-01-01",
        "adg": 0.001,
        "gain": 1.2,
        "drawdown_worst": 0.72,
        "sharpe_ratio": 0.3,
        "analysis": {
            "adg": 0.001,
            "gain": 1.2,
            "drawdown_worst": 0.72,
            "sharpe_ratio": 0.3,
            "sortino_ratio": 0.4,
            "omega_ratio": 0.9,
            "equity_choppiness": 0.8,
            "equity_jerkiness": 0.7,
            "backtest_completion_ratio": 1.0,
        },
    }

    scored = score_archive_results([risky, good])

    by_name = {item["config_name"]: item for item in scored}
    assert by_name["good"]["pbgui_score"]["value"] > by_name["risky"]["pbgui_score"]["value"]
    assert by_name["risky"]["pbgui_score"]["value"] <= 4.0
    assert "very_high_drawdown" in by_name["risky"]["pbgui_score"]["flags"]


def test_archive_scores_liquidated_result_is_one() -> None:
    """Liquidated archive results always receive the minimum score."""
    scored = score_archive_results([
        {
            "config_name": "liq",
            "result_name": "2026-06-21T120000Z",
            "exchange_dir": "bybit",
            "display_name": "liq/result",
            "liquidated": True,
            "start_date": "2025-01-01",
            "end_date": "2026-01-01",
            "analysis": {"adg": 0.01, "gain": 10, "drawdown_worst": 0.99, "sharpe_ratio": 3.0},
        }
    ])

    assert scored[0]["pbgui_score"]["value"] == 1.0
    assert "liquidated" in scored[0]["pbgui_score"]["flags"]


def test_archive_scores_cap_elevated_drawdown() -> None:
    """Results with 40%+ drawdown cannot rank as top-tier scores."""
    scored = score_archive_results([
        {
            "config_name": "too_deep",
            "result_name": "2026-06-21T120000Z",
            "exchange_dir": "bybit",
            "display_name": "too_deep/result",
            "start_date": "2025-01-01",
            "end_date": "2026-01-01",
            "analysis": {
                "adg": 0.01,
                "mdg": 0.01,
                "gain": 10.0,
                "drawdown_worst": 0.44,
                "equity_balance_diff_neg_max": 0.43,
                "sharpe_ratio": 3.0,
                "sortino_ratio": 4.0,
                "omega_ratio": 2.0,
                "calmar_ratio": 3.0,
                "sterling_ratio": 3.0,
                "win_rate": 1.0,
                "equity_choppiness": 0.1,
                "equity_jerkiness": 0.1,
                "exponential_fit_error": 0.001,
                "backtest_completion_ratio": 1.0,
            },
        }
    ])

    score = scored[0]["pbgui_score"]
    assert score["value"] <= 6.5
    assert "elevated_drawdown" in score["flags"]
    assert "elevated_equity_balance_diff" in score["flags"]


def test_update_archive_scores_writes_manifest_and_readme_score_block(tmp_path: Path) -> None:
    """Score rebuild writes scores into the manifest and updates README score table."""
    archive_root = tmp_path / "archive"
    result = write_archive_result(archive_root / "pbgui/configs/v7.4.2/backtests")
    write_archive_json(result / "analysis.json", {
        "adg": 0.004,
        "gain": 2.0,
        "drawdown_worst": 0.15,
        "sharpe_ratio": 1.5,
        "sortino_ratio": 2.0,
        "backtest_completion_ratio": 1.0,
    })
    write_archive_json(result / "dataset.json", {"coins": ["HYPE"]})
    save_archive_readme_config(archive_root, {"title": "Score Archive", "static_markdown": "Static intro."})

    payload = update_archive_scores_and_readme(archive_root)

    manifest = load_archive_manifest(archive_root)
    readme = (archive_root / "README.md").read_text(encoding="utf-8")
    scores_page = (archive_root / "SCORES.md").read_text(encoding="utf-8")
    scores_html = (archive_root / "SCORES.html").read_text(encoding="utf-8")
    result_items = [item for item in manifest["items"] if item.get("type") == "backtest_result"]
    assert payload["scored"] == 1
    assert result_items[0]["score"]["version"] == 2
    assert result_items[0]["score"]["value"] >= 1.0
    assert "PBGui Score Overview" in readme
    assert "[Open interactive score table](SCORES.html)" in readme
    assert "[Open Markdown score table](SCORES.md)" in readme
    assert "Top 10 Scores" in readme
    assert "| Score | Config |" in readme
    assert '<table width="100%">' not in readme
    assert "PBGui Score Table" in scores_page
    assert '<table width="100%">' in scores_page
    assert '<th width="52%">Config</th>' in scores_page
    assert 'title="' in scores_page
    assert '<th width="14%">Result</th>' not in scores_page
    assert "PBGui Score Table" in scores_html
    assert "score-data" in scores_html
    assert ">Conf</th>" not in scores_html
    assert 'data-key="confidence"' not in scores_html
    assert "coin-filter" in scores_html
    assert 'data-key="coins_text"' in scores_html
    assert '"coins":["HYPE"]' in scores_html
    assert '"coins_text":"HYPE"' in scores_html
    assert "addEventListener('click'" in scores_html
    assert "Static intro." in readme


def test_archive_score_preview_contains_full_readme_markdown(tmp_path: Path) -> None:
    """Score preview returns the full README Markdown PBGui would generate."""
    archive_root = tmp_path / "archive"
    write_archive_result(archive_root / "pbgui/configs/v7.4.2/backtests")
    save_archive_readme_config(archive_root, {"title": "Preview Archive", "static_markdown": "Static preview intro."})

    payload = build_archive_score_payload(archive_root)

    assert payload["readme_markdown"].startswith("# Preview Archive")
    assert "Static preview intro." in payload["readme_markdown"]
    assert "PBGui Score Overview" in payload["readme_markdown"]
    assert "[Open interactive score table](SCORES.html)" in payload["readme_markdown"]
    assert "Top 10 Scores" in payload["readme_markdown"]
    assert payload["scores_html_path"] == "SCORES.html"
    assert "score-data" in payload["scores_page_html"]
    assert "PBGui Score Table" in payload["scores_page_markdown"]
    assert "pbgui:scores:start" in payload["readme_markdown"]


def test_archive_github_links_target_pages_and_repo_tree(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """GitHub Pages score links use Pages for HTML and GitHub tree links for result folders."""
    remote = "https://github.com/msei99/pbconfigs.git"
    path = "pbgui/configs/v7.12.0/backtests/cfg/combined/result"
    expected_tree = "https://github.com/msei99/pbconfigs/tree/main/" + path

    assert archive_helpers.github_pages_base_url(remote) == "https://msei99.github.io/pbconfigs/"
    assert archive_helpers.github_repo_base_url(remote) == "https://github.com/msei99/pbconfigs"
    monkeypatch.setattr(archive_helpers, "archive_git_remote_url", lambda _root: remote)
    monkeypatch.setattr(archive_helpers, "archive_git_branch", lambda _root: "main")
    assert archive_helpers.archive_github_tree_url(tmp_path, path) == expected_tree

    html_page = archive_helpers.build_archive_scores_html([{
        "config_name": "cfg",
        "result_name": "result",
        "display_name": path,
        "exchange_dir": "combined",
        "pbgui_score": {"value": 5.0, "confidence": 0.9, "flags": []},
    }], tmp_path)

    assert expected_tree in html_page


def test_archive_score_markdown_includes_all_rows_by_default() -> None:
    """Generated README score Markdown must include the full result set, not only a preview slice."""
    scored = []
    for idx in range(60):
        scored.append({
            "config_name": f"cfg-{idx:03d}",
            "exchange_dir": "combined",
            "result_name": f"result-{idx:03d}",
            "display_name": f"cfg/result-{idx:03d}",
            "adg": 0.001,
            "gain": 1.0,
            "drawdown_worst": 0.1,
            "sharpe_ratio": 1.0,
            "pbgui_score": {"value": 5.0, "confidence": 0.9, "flags": []},
        })

    markdown = build_archive_scores_markdown(scored)

    assert "result-000" in markdown
    assert "result-059" in markdown


def test_archive_storage_estimate_reports_saved_space(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Archive compaction preview reports estimated before/after/saved object storage."""
    archive_root = tmp_path / "archive"
    archive_root.mkdir()

    def fake_run(cmd, cwd=None, capture_output=True, text=True, timeout=None, input=None, env=None):
        if cmd[:3] == ["git", "count-objects", "-v"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="size: 1024\nsize-pack: 3072\nsize-garbage: 0\n", stderr="")
        if cmd[:3] == ["git", "rev-parse", "HEAD^{tree}"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="tree1\n", stderr="")
        if cmd[:4] == ["git", "ls-tree", "-r", "-t"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="100644 blob blob1\tconfig.json\x00", stderr="")
        if cmd[:2] == ["git", "cat-file"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="tree1 256\nblob1 768\n", stderr="")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(backtest_v7, "_log_archive", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(subprocess, "run", fake_run)

    estimate = backtest_v7._archive_storage_estimate("mine", archive_root)

    assert estimate["available"] is True
    assert estimate["current_bytes"] == 4 * 1024 * 1024
    assert estimate["after_bytes"] == 1024
    assert estimate["saved_bytes"] == (4 * 1024 * 1024) - 1024
    assert estimate["saved_percent"] == 100.0


def test_archive_git_env_forces_english_cli_output() -> None:
    """Archive Git commands should not inherit localized CLI messages."""
    env = backtest_v7._archive_git_env()

    assert env["LC_ALL"] == "C"
    assert env["LANG"] == "C"
    assert env["LANGUAGE"] == "C"


def test_archive_pull_recovers_clean_foreign_archive_after_force_update(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Foreign archive pulls recover clean clones after compacted remote history."""
    commands = []

    def fake_run(cmd, cwd=None, capture_output=True, text=True, timeout=None, input=None, env=None):
        commands.append(list(cmd))
        if cmd == ["git", "pull", "--ff-only"]:
            return subprocess.CompletedProcess(cmd, 128, stdout="", stderr="fatal: Not possible to fast-forward\n")
        if cmd == ["git", "status", "--porcelain"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        if cmd == ["git", "branch", "--show-current"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="main\n", stderr="")
        if cmd == ["git", "fetch", "origin", "main"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="forced update\n")
        if cmd == ["git", "rev-parse", "--verify", "origin/main"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="remote-sha\n", stderr="")
        if len(cmd) == 4 and cmd[:2] == ["git", "branch"] and cmd[2].startswith("pbgui-recovery-") and cmd[3] == "HEAD":
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        if cmd == ["git", "reset", "--hard", "origin/main"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="HEAD is now at remote-sha\n", stderr="")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")
    monkeypatch.setattr(backtest_v7, "_log_archive", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(subprocess, "run", fake_run)

    result = backtest_v7._archive_pull_sync("other", tmp_path)

    assert result["error"] == ""
    assert result["recovered"] is True
    assert commands[:2] == [["git", "status", "--porcelain"], ["git", "pull", "--ff-only"]]
    assert any(len(cmd) == 4 and cmd[:2] == ["git", "branch"] and cmd[2].startswith("pbgui-recovery-") for cmd in commands)
    assert ["git", "reset", "--hard", "origin/main"] in commands


def test_archive_pull_blocks_foreign_local_layout_migration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Foreign archive pulls must not reset or clean a dirty local layout migration."""
    commands = []

    def fake_run(cmd, cwd=None, capture_output=True, text=True, timeout=None, input=None, env=None):
        commands.append(list(cmd))
        if cmd == ["git", "pull", "--ff-only"]:
            return subprocess.CompletedProcess(cmd, 128, stdout="", stderr="fatal: Not possible to fast-forward\n")
        if cmd == ["git", "status", "--porcelain"]:
            return subprocess.CompletedProcess(cmd, 0, stdout=" D v7.3/config/binance/run/config.json\n?? pbgui/\n", stderr="")
        if cmd == ["git", "reset", "--hard", "HEAD"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="HEAD is now at local-sha\n", stderr="")
        if cmd == ["git", "clean", "-fd", "--", "pbgui"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="Removing pbgui/\n", stderr="")
        if cmd == ["git", "branch", "--show-current"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="main\n", stderr="")
        if cmd == ["git", "fetch", "origin", "main"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="forced update\n")
        if cmd == ["git", "rev-parse", "--verify", "origin/main"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="remote-sha\n", stderr="")
        if len(cmd) == 4 and cmd[:2] == ["git", "branch"] and cmd[2].startswith("pbgui-recovery-") and cmd[3] == "HEAD":
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        if cmd == ["git", "reset", "--hard", "origin/main"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="HEAD is now at remote-sha\n", stderr="")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")
    monkeypatch.setattr(backtest_v7, "_log_archive", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(subprocess, "run", fake_run)

    result = backtest_v7._archive_pull_sync("other", tmp_path)

    assert "local changes" in result["error"]
    assert "read-only" in result["error"]
    assert result["recovered"] is False
    assert commands == [["git", "status", "--porcelain"]]
    assert ["git", "reset", "--hard", "HEAD"] not in commands
    assert ["git", "clean", "-fd", "--", "pbgui"] not in commands
    assert ["git", "reset", "--hard", "origin/main"] not in commands


def test_archive_pull_does_not_reset_own_archive_after_divergence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Own archive pulls must not discard local commits after divergence."""
    commands = []

    def fake_run(cmd, cwd=None, capture_output=True, text=True, timeout=None, input=None, env=None):
        commands.append(list(cmd))
        if cmd == ["git", "pull", "--ff-only"]:
            return subprocess.CompletedProcess(cmd, 128, stdout="", stderr="fatal: Not possible to fast-forward\n")
        if cmd == ["git", "status", "--porcelain"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")
    monkeypatch.setattr(backtest_v7, "_log_archive", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(subprocess, "run", fake_run)

    result = backtest_v7._archive_pull_sync("mine", tmp_path)

    assert result["error"]
    assert result["recovered"] is False
    assert commands == [["git", "status", "--porcelain"], ["git", "pull", "--ff-only"]]


def test_archive_pull_blocks_dirty_own_archive_before_network_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Own archive pulls require local content to be committed and pushed first."""
    commands = []
    rebuilt = []

    def fake_run(cmd, cwd=None, capture_output=True, text=True, timeout=None, input=None, env=None):
        commands.append(list(cmd))
        if cmd == ["git", "pull", "--ff-only"]:
            raise AssertionError("dirty archive must not pull")
        if cmd == ["git", "status", "--porcelain"]:
            return subprocess.CompletedProcess(
                cmd,
                0,
                stdout=" M pbgui/archive_manifest.json\n?? pbgui/configs/v7.12.0/optimize/sl_bt_twe_target.json\n?? pbgui/configs/v7.12.0/optimize/sl_bt_twe_target.meta.json\n",
                stderr="",
            )
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")
    monkeypatch.setattr(backtest_v7, "_log_archive", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(backtest_v7, "rebuild_archive_manifest", lambda dest: rebuilt.append(dest) or {"items": []})
    monkeypatch.setattr(subprocess, "run", fake_run)

    result = backtest_v7._archive_pull_sync("mine", tmp_path)

    assert "commit and push" in result["error"]
    assert result["recovered"] is False
    assert result["conflict"] is True
    assert commands == [["git", "status", "--porcelain"]]
    assert rebuilt == []


def test_archive_push_pre_pull_preserves_local_archive_content(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Archive push pre-pull handles generated metadata without discarding local configs."""
    commands = []
    pull_count = 0
    rebuilt = []
    log_lines = []

    def fake_run(cmd, cwd=None, capture_output=True, text=True, timeout=None, input=None, env=None):
        nonlocal pull_count
        commands.append(list(cmd))
        if cmd == ["git", "pull"]:
            pull_count += 1
            if pull_count == 1:
                return subprocess.CompletedProcess(cmd, 1, stdout="Updating remote changes\n", stderr="error: Your local changes would be overwritten\n")
            return subprocess.CompletedProcess(cmd, 0, stdout="Merge made by recursive\n", stderr="")
        if cmd == ["git", "status", "--porcelain"]:
            return subprocess.CompletedProcess(
                cmd,
                0,
                stdout=" M pbgui/archive_manifest.json\n?? pbgui/configs/v7.12.0/optimize/sl_bt_twe_target.json\n",
                stderr="",
            )
        if cmd == ["git", "checkout", "--", "pbgui/archive_manifest.json"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")
    monkeypatch.setattr(backtest_v7, "_log_archive", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(backtest_v7, "rebuild_archive_manifest", lambda dest: rebuilt.append(dest) or {"items": []})
    monkeypatch.setattr(subprocess, "run", fake_run)

    backtest_v7._archive_pre_push_pull("mine", tmp_path, "secret", log_lines)

    assert pull_count == 2
    assert ["git", "checkout", "--", "pbgui/archive_manifest.json"] in commands
    assert rebuilt == [tmp_path]
    assert any("preserving local archive content" in line for line in log_lines)


def test_archive_pull_does_not_reset_dirty_foreign_archive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Foreign archive pulls do not auto-reset if local files were changed."""
    commands = []

    def fake_run(cmd, cwd=None, capture_output=True, text=True, timeout=None, input=None, env=None):
        commands.append(list(cmd))
        if cmd == ["git", "pull", "--ff-only"]:
            return subprocess.CompletedProcess(cmd, 128, stdout="", stderr="fatal: Not possible to fast-forward\n")
        if cmd == ["git", "status", "--porcelain"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="M pbgui/readme_config.json\n", stderr="")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")
    monkeypatch.setattr(backtest_v7, "_log_archive", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(subprocess, "run", fake_run)

    result = backtest_v7._archive_pull_sync("other", tmp_path)

    assert "local changes" in result["error"]
    assert "read-only" in result["error"]
    assert result["recovered"] is False
    assert commands == [["git", "status", "--porcelain"]]
    assert ["git", "reset", "--hard", "origin/main"] not in commands


def test_archive_pull_fails_closed_when_status_preflight_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed status preflight prevents pull and all recovery commands."""
    commands = []

    def fake_run(cmd, cwd=None, capture_output=True, text=True, timeout=None, input=None, env=None):
        commands.append(list(cmd))
        if cmd == ["git", "status", "--porcelain"]:
            return subprocess.CompletedProcess(cmd, 128, stdout="", stderr="fatal: status failed\n")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(backtest_v7, "_log_archive", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(subprocess, "run", fake_run)

    result = backtest_v7._archive_pull_sync("other", tmp_path)

    assert "status failed" in result["error"]
    assert commands == [["git", "status", "--porcelain"]]


def test_archive_pull_route_maps_dirty_conflict_to_409(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The single-archive pull route exposes a dirty preflight as HTTP 409."""
    archives_root = tmp_path / "archives"
    (archives_root / "mine").mkdir(parents=True)
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(
        backtest_v7,
        "_archive_pull_sync",
        lambda *_args: {
            "name": "mine",
            "output": "dirty",
            "error": "Own archive has local changes; commit and push them before pulling.",
            "recovered": False,
            "conflict": True,
        },
    )

    with pytest.raises(HTTPException) as exc_info:
        backtest_v7.git_pull("mine", session=None)

    assert exc_info.value.status_code == 409


def test_archive_pull_stream_emits_conflict_before_network_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Streaming pull reports a dirty conflict without invoking pull or recovery."""
    commands = []

    def fake_stream_step(name, dest, label, cmd, **kwargs):
        commands.append(list(cmd))
        if cmd != ["git", "status", "--porcelain"]:
            raise AssertionError(f"unexpected command: {cmd}")
        if False:
            yield ""
        return subprocess.CompletedProcess(cmd, 0, stdout=" M README.md\n", stderr=""), " M README.md"

    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")
    monkeypatch.setattr(backtest_v7, "_log_archive", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(backtest_v7, "_run_archive_git_stream_step", fake_stream_step)

    events = [json.loads(line) for line in backtest_v7._archive_pull_stream_sync("mine", tmp_path)]

    assert commands == [["git", "status", "--porcelain"]]
    assert [event["type"] for event in events] == ["conflict", "archive_done"]
    assert events[-1]["result"]["conflict"] is True


def test_pull_all_archives_stream_yields_final_results(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pull-all stream emits per-archive progress and a final done event."""
    archive_root = tmp_path / "archives"
    (archive_root / "other" / ".git").mkdir(parents=True)
    (archive_root / "other" / ".git" / "config").write_text("[remote]\n", encoding="utf-8")

    def fake_pull_stream(name: str, dest: Path):
        yield backtest_v7._archive_stream_event("status", archive=name, message="git pull started")
        return {"name": name, "output": "ok", "error": "", "recovered": False}

    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archive_root)
    monkeypatch.setattr(backtest_v7, "_archive_pull_stream_sync", fake_pull_stream)

    events = [json.loads(line) for line in backtest_v7._pull_all_archives_stream_sync()]

    assert events[0]["type"] == "archive_start"
    assert events[1]["type"] == "status"
    assert events[-1]["type"] == "done"
    assert events[-1]["ok"] is True
    assert events[-1]["results"] == [{"name": "other", "output": "ok", "error": "", "recovered": False}]


def test_compact_archive_uses_force_with_lease_sequence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Archive compaction creates a root commit and force-pushes it with an explicit lease."""
    archives_root = tmp_path / "archives"
    archive_root = archives_root / "mine"
    archive_root.mkdir(parents=True)
    commands = []

    def fake_run(cmd, cwd=None, capture_output=True, text=True, timeout=None, env=None):
        commands.append(list(cmd))
        if cmd[:3] == ["git", "remote", "get-url"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="https://example.invalid/repo.git\n", stderr="")
        if cmd[:3] == ["git", "branch", "--show-current"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="main\n", stderr="")
        if cmd[:2] == ["git", "fetch"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        if cmd[:3] == ["git", "rev-parse", "FETCH_HEAD"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="oldremote\n", stderr="")
        if cmd[:3] == ["git", "rev-list", "--left-right"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="2\t0\n", stderr="")
        if cmd[:3] == ["git", "add", "-A"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        if cmd[:2] == ["git", "write-tree"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="tree123\n", stderr="")
        if cmd[:2] == ["git", "commit-tree"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="newcommit\n", stderr="")
        if cmd[:2] == ["git", "push"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="pushed\n", stderr="")
        if cmd[:2] == ["git", "update-ref"]:
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")
    monkeypatch.setattr(backtest_v7, "rebuild_archive_manifest", lambda _root: {"schema_version": 1, "items": [{}, {}]})
    monkeypatch.setattr(backtest_v7, "_invalidate_archive_cache", lambda _name=None: None)
    monkeypatch.setattr(backtest_v7, "_log_archive", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(subprocess, "run", fake_run)

    response = backtest_v7.compact_archive_history(
        "mine",
        {"dry_run": False, "access_token": "tok"},
        session=None,
    )

    assert response["commit"] == "newcommit"
    fetch_idx = next(i for i, cmd in enumerate(commands) if cmd[:2] == ["git", "fetch"])
    add_idx = next(i for i, cmd in enumerate(commands) if cmd[:3] == ["git", "add", "-A"])
    write_tree_idx = next(i for i, cmd in enumerate(commands) if cmd[:2] == ["git", "write-tree"])
    commit_tree_idx = next(i for i, cmd in enumerate(commands) if cmd[:2] == ["git", "commit-tree"])
    push_idx = next(i for i, cmd in enumerate(commands) if cmd[:2] == ["git", "push"])
    update_idx = next(i for i, cmd in enumerate(commands) if cmd[:2] == ["git", "update-ref"])
    assert fetch_idx < add_idx < write_tree_idx < commit_tree_idx < push_idx < update_idx
    push_cmd = commands[push_idx]
    assert "--force-with-lease=refs/heads/main:oldremote" in push_cmd
    assert "newcommit:refs/heads/main" in push_cmd


def test_archive_retest_until_yesterday_keeps_original_window() -> None:
    """Archive retests can keep the original window length while ending yesterday."""
    cfg = {"backtest": {"start_date": "2026-01-01", "end_date": "2026-01-10"}}

    meta = backtest_v7._apply_archive_retest_date_policy(
        cfg,
        {"date_mode": "until_yesterday", "last_days": 30},
        today=datetime.date(2026, 6, 21),
    )

    assert meta == {
        "date_mode": "until_yesterday",
        "window_days": 10,
        "start_date": "2026-06-11",
        "end_date": "2026-06-20",
    }
    assert cfg["backtest"]["start_date"] == "2026-06-11"
    assert cfg["backtest"]["end_date"] == "2026-06-20"


def test_archive_retest_last_x_days_ends_yesterday() -> None:
    """Last-X-days retests also always end at yesterday."""
    cfg = {"backtest": {"start_date": "2020-01-01", "end_date": "2020-12-31"}}

    meta = backtest_v7._apply_archive_retest_date_policy(
        cfg,
        {"date_mode": "last_x_days", "last_days": 7},
        today=datetime.date(2026, 6, 21),
    )

    assert meta["date_mode"] == "last_x_days"
    assert meta["window_days"] == 7
    assert cfg["backtest"]["start_date"] == "2026-06-14"
    assert cfg["backtest"]["end_date"] == "2026-06-20"


def test_archive_retest_replace_removes_old_result_after_copy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Retest replacement copies the new result before removing the old archive result."""
    archives_root = tmp_path / "archives"
    archive_root = archives_root / "mine"
    old_result = write_archive_result(archive_root / "pbgui/configs/v7.4.2/backtests")
    local_results_root = tmp_path / "local_results"
    queue_dir = tmp_path / "queue"
    log_dir = tmp_path / "logs" / "backtests"
    queue_config_dir = tmp_path / "archive_retests" / "queue_configs"
    queue_name = "archive_retest_my_config_abcdef12"
    queue_filename = "queue123"
    queue_config = queue_config_dir / "run123.json"
    new_result = local_results_root / queue_name / "my_config" / "bybit" / "2026-06-22T120000Z"
    config = {
        "config_version": "v7.4.2",
        "backtest": {
            "base_dir": f"backtests/pbgui/{queue_name}",
            "exchanges": ["bybit"],
            "starting_balance": 1000,
        },
        "bot": {"long": {}, "short": {}},
        "pbgui": {"archive_retest": {"run_id": "run123"}},
    }
    write_archive_json(new_result / "config.json", config)
    write_archive_json(new_result / "analysis.json", {"gain": 1.3, "liquidated": False})
    write_archive_json(queue_dir / f"{queue_filename}.json", {"filename": queue_filename})
    (queue_dir / f"{queue_filename}.pid").write_text("123", encoding="utf-8")
    (log_dir).mkdir(parents=True, exist_ok=True)
    (log_dir / f"{queue_filename}.log").write_text("seconds elapsed for backtest: 1", encoding="utf-8")
    write_archive_json(queue_config, config)
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")
    monkeypatch.setattr(backtest_v7, "_bt_results_base", lambda: str(local_results_root))
    monkeypatch.setattr(backtest_v7, "_bt_queue_dir", lambda: queue_dir)
    monkeypatch.setattr(backtest_v7, "_bt_log_dir", lambda: log_dir)
    monkeypatch.setattr(backtest_v7, "_archive_retest_queue_configs_dir", lambda: queue_config_dir)

    response = backtest_v7._replace_archive_result_from_local(
        {
            "id": "run123",
            "archive_name": "mine",
            "source_relative_path": old_result.relative_to(archive_root).as_posix(),
            "archive_config_name": "my_config",
            "queue_name": queue_name,
            "queue_filename": queue_filename,
            "queue_config": str(queue_config),
            "options": {"skip_liquidated": True},
        }
    )

    assert not old_result.exists()
    assert response["new_relative_path"] == "pbgui/configs/v7.4.2/backtests/my_config/bybit/2026-06-22T120000Z"
    assert (archive_root / response["new_relative_path"] / "analysis.json").exists()
    assert response["manifest"]["schema_version"] == 1
    assert not (local_results_root / queue_name).exists()
    assert not (queue_dir / f"{queue_filename}.json").exists()
    assert not (queue_dir / f"{queue_filename}.pid").exists()
    assert not (log_dir / f"{queue_filename}.log").exists()
    assert not queue_config.exists()
    assert not response["cleanup"]["errors"]


def test_archive_retest_completion_backfills_cleanup_for_completed_runs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Already-completed archive retests without cleanup metadata should be cleaned on the next worker pass."""
    local_results_root = tmp_path / "local_results"
    queue_dir = tmp_path / "queue"
    log_dir = tmp_path / "logs" / "backtests"
    queue_config_dir = tmp_path / "archive_retests" / "queue_configs"
    queue_name = "archive_retest_my_config_abcdef12"
    queue_filename = "queue123"
    queue_config = queue_config_dir / "run123.json"
    result_dir = local_results_root / queue_name / "my_config" / "bybit" / "2026-06-22T120000Z"
    config = {
        "config_version": "v7.4.2",
        "backtest": {"base_dir": f"backtests/pbgui/{queue_name}", "exchanges": ["bybit"]},
        "bot": {"long": {}, "short": {}},
        "pbgui": {"archive_retest": {"run_id": "run123"}},
    }
    write_archive_json(result_dir / "config.json", config)
    write_archive_json(result_dir / "analysis.json", {"gain": 1.3, "liquidated": False})
    write_archive_json(queue_dir / f"{queue_filename}.json", {"filename": queue_filename})
    (queue_dir / f"{queue_filename}.pid").write_text("123", encoding="utf-8")
    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / f"{queue_filename}.log").write_text("seconds elapsed for backtest: 1", encoding="utf-8")
    write_archive_json(queue_config, config)
    runs = [
        {
            "id": "run123",
            "status": "complete",
            "queue_name": queue_name,
            "queue_filename": queue_filename,
            "queue_config": str(queue_config),
            "result": {"new_relative_path": "pbgui/configs/v7.4.2/backtests/my_config/bybit/2026-06-22T120000Z"},
        }
    ]
    saved = {}
    monkeypatch.setattr(backtest_v7, "_bt_results_base", lambda: str(local_results_root))
    monkeypatch.setattr(backtest_v7, "_bt_queue_dir", lambda: queue_dir)
    monkeypatch.setattr(backtest_v7, "_bt_log_dir", lambda: log_dir)
    monkeypatch.setattr(backtest_v7, "_archive_retest_queue_configs_dir", lambda: queue_config_dir)
    monkeypatch.setattr(backtest_v7, "_load_archive_retest_runs", lambda: runs)
    monkeypatch.setattr(backtest_v7, "_save_archive_retest_runs", lambda value: saved.setdefault("runs", value))
    monkeypatch.setattr(backtest_v7, "_log", lambda *args, **kwargs: None)

    backtest_v7._process_archive_retest_completions()

    assert not (local_results_root / queue_name).exists()
    assert not (queue_dir / f"{queue_filename}.json").exists()
    assert not (queue_dir / f"{queue_filename}.pid").exists()
    assert not (log_dir / f"{queue_filename}.log").exists()
    assert not queue_config.exists()
    assert saved["runs"][0]["result"]["cleanup"]["errors"] == []


@pytest.mark.parametrize(
    "operation",
    ["delete_result", "add_backtest", "add_optimize", "migrate", "remove_duplicates"],
)
def test_foreign_archive_content_mutations_are_rejected(
    operation: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Archive content mutations must stop at the ownership guard for foreign archives."""
    archives_root = tmp_path / "archives"
    foreign_root = archives_root / "other"
    result_dir = write_archive_result(foreign_root / "pbgui/configs/v7.4.2/backtests")
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")

    if operation == "delete_result":
        call = lambda: backtest_v7.delete_archive_result("other", str(result_dir), session=None)
    elif operation == "add_backtest":
        call = lambda: asyncio.run(
            backtest_v7.add_config_to_archive("other", {"source_path": str(result_dir)}, session=None)
        )
    elif operation == "add_optimize":
        call = lambda: asyncio.run(
            backtest_v7.add_optimize_config_to_archive("other", {"config_name": "demo"}, session=None)
        )
    elif operation == "migrate":
        call = lambda: backtest_v7.migrate_archive("other", session=None)
    else:
        call = lambda: backtest_v7.remove_archive_duplicate_results(
            "other", {"paths": [str(result_dir)], "dry_run": True}, session=None
        )

    with pytest.raises(HTTPException) as exc_info:
        call()

    assert exc_info.value.status_code == 403
    assert result_dir.exists()


@pytest.mark.parametrize(
    "operation",
    [
        "delete_result",
        "rename",
        "push",
        "compact",
        "scores",
        "add_backtest_sync",
        "migrate",
        "add_optimize_sync",
        "delete_optimize",
        "retest_replace",
        "schedule",
        "remove_liquidated",
        "remove_duplicates",
        "save_readme",
    ],
)
def test_own_archive_content_mutations_validate_inside_transaction(
    operation: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every own-archive content workflow revalidates ownership and existence under its path lock."""
    archives_root = tmp_path / "archives"
    candidate = archives_root / "mine"
    candidate.mkdir(parents=True)
    state = {"active": False, "entries": 0}

    class ValidationReached(Exception):
        """Stop a workflow once its in-transaction ownership validation runs."""

    class Transaction:
        """Track transaction entry around the ownership guard."""

        def __init__(self, root: Path):
            assert root == candidate

        def __enter__(self):
            state["active"] = True
            state["entries"] += 1

        def __exit__(self, *_args):
            state["active"] = False

    def require_locked(name: str, _action: str) -> Path:
        """Assert the shared guard runs only after the candidate transaction is entered."""
        assert name == "mine"
        assert state["active"] is True
        raise ValidationReached

    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "archive_transaction", Transaction)
    monkeypatch.setattr(backtest_v7, "_require_own_archive", require_locked)
    calls = {
        "delete_result": lambda: backtest_v7.delete_archive_result("mine", "unused", session=None),
        "rename": lambda: backtest_v7.rename_archive_backtest_config("mine", {}, session=None),
        "push": lambda: backtest_v7.git_push("mine", {}, session=None),
        "compact": lambda: backtest_v7.compact_archive_history("mine", {}, session=None),
        "scores": lambda: backtest_v7.rebuild_archive_scores("mine", session=None),
        "add_backtest_sync": lambda: backtest_v7._add_config_to_archive_sync("mine", "unused"),
        "migrate": lambda: backtest_v7.migrate_archive("mine", session=None),
        "add_optimize_sync": lambda: backtest_v7._add_optimize_config_to_archive_sync("mine", "unused"),
        "delete_optimize": lambda: backtest_v7.delete_archive_optimize_config("mine", "unused", session=None),
        "retest_replace": lambda: backtest_v7.retest_replace_archive_results("mine", {}, session=None),
        "schedule": lambda: backtest_v7.create_archive_retest_schedule("mine", {}, session=None),
        "remove_liquidated": lambda: backtest_v7.remove_archive_liquidated_results("mine", {}, session=None),
        "remove_duplicates": lambda: backtest_v7.remove_archive_duplicate_results("mine", {}, session=None),
        "save_readme": lambda: backtest_v7.save_archive_readme_settings("mine", {}, session=None),
    }

    with pytest.raises(ValidationReached):
        calls[operation]()

    assert state == {"active": False, "entries": 1}


@pytest.mark.parametrize("operation", ["list_results", "pull", "pull_stream"])
def test_archive_workflows_recheck_root_after_transaction_entry(
    operation: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Archive workflows stop inside the transaction if deletion removed the candidate first."""
    archives_root = tmp_path / "archives"
    candidate = archives_root / "mine"
    candidate.mkdir(parents=True)
    entered = []

    class DeleteBeforeEntry:
        """Simulate deletion winning immediately before the waiting workflow acquires the lock."""

        def __init__(self, root: Path):
            assert root == candidate

        def __enter__(self):
            candidate.rmdir()
            entered.append(candidate)

        def __exit__(self, *_args):
            return None

    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "archive_transaction", DeleteBeforeEntry)
    monkeypatch.setattr(backtest_v7, "_get_cached_archive_results", lambda _name: None)
    monkeypatch.setattr(
        backtest_v7,
        "maybe_migrate_own_archive",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("missing archive must not be mutated")),
    )
    monkeypatch.setattr(
        backtest_v7,
        "_run_archive_git_step",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("missing archive must not run Git")),
    )
    monkeypatch.setattr(
        backtest_v7,
        "_run_archive_git_stream_step",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("missing archive must not run Git")),
    )

    if operation == "list_results":
        with pytest.raises(HTTPException) as exc_info:
            backtest_v7.list_archive_results("mine", session=None)
        assert exc_info.value.status_code == 404
    elif operation == "pull":
        response = backtest_v7._archive_pull_sync("mine", candidate)
        assert response["error"] == "Archive 'mine' not found"
    else:
        events = [json.loads(line) for line in backtest_v7._archive_pull_stream_sync("mine", candidate)]
        assert [event["type"] for event in events] == ["error", "archive_done"]
        assert events[-1]["result"]["error"] == "Archive 'mine' not found"

    assert entered == [candidate]


@pytest.mark.parametrize("dry_run", [False, True])
def test_archive_push_rejects_foreign_archive_before_git(
    dry_run: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both real and dry-run pushes are restricted to the configured own archive."""
    archives_root = tmp_path / "archives"
    (archives_root / "other").mkdir(parents=True)
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")
    monkeypatch.setattr(
        backtest_v7.subprocess,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("git must not run")),
    )

    with pytest.raises(HTTPException) as exc_info:
        backtest_v7.git_push("other", {"dry_run": dry_run}, session=None)

    assert exc_info.value.status_code == 403


def test_archive_push_dry_run_holds_archive_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Credential-check pushes remain serialized for the complete dry-run."""
    archives_root = tmp_path / "archives"
    archive_root = archives_root / "mine"
    archive_root.mkdir(parents=True)
    state = {"active": False, "entries": 0}
    commands = []

    class Transaction:
        """Track the transaction lifetime around the mocked Git command."""

        def __enter__(self):
            state["active"] = True
            state["entries"] += 1

        def __exit__(self, *_args):
            state["active"] = False

    def fake_git_step(name, dest, label, cmd, **kwargs):
        assert state["active"] is True
        commands.append(list(cmd))
        return subprocess.CompletedProcess(cmd, 0, stdout="ok", stderr=""), "ok"

    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")
    monkeypatch.setattr(backtest_v7, "archive_transaction", lambda _root: Transaction())
    monkeypatch.setattr(backtest_v7, "_archive_push_url", lambda *_args: None)
    monkeypatch.setattr(backtest_v7, "_run_archive_git_step", fake_git_step)
    monkeypatch.setattr(backtest_v7, "_log_archive", lambda *_args, **_kwargs: None)

    response = backtest_v7.git_push("mine", {"dry_run": True}, session=None)

    assert response["ok"] is True
    assert state == {"active": False, "entries": 1}
    assert commands == [["git", "push", "--dry-run"]]


def test_archive_results_panel_uses_cache_before_bounded_migration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A panel cache hit bypasses migration, while a miss limits migration to 25 items."""
    archives_root = tmp_path / "archives"
    archive_root = archives_root / "mine"
    archive_root.mkdir(parents=True)
    cached_entry = [{
        "results": [{"path": "cached"}],
        "migration_status": {"status": "cached"},
    }]
    migration_calls = []
    cached_writes = []
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")
    monkeypatch.setattr(backtest_v7, "_get_cached_archive_results", lambda _name: cached_entry[0])

    def fake_migrate(name: str, root: Path, own_name: str, max_items: int | None = None) -> dict:
        """Record the panel's bounded migration request."""
        migration_calls.append((name, root, own_name, max_items))
        return {"status": {"status": "current"}}

    monkeypatch.setattr(backtest_v7, "maybe_migrate_own_archive", fake_migrate)
    monkeypatch.setattr(backtest_v7, "list_archive_backtest_results", lambda _root: [{"path": "fresh"}])
    monkeypatch.setattr(backtest_v7, "score_archive_results", lambda results: results)
    monkeypatch.setattr(
        backtest_v7,
        "_set_cached_archive_results",
        lambda name, results, status: cached_writes.append((name, results, status)),
    )

    cached_response = backtest_v7.list_archive_results("mine", session=None)
    assert cached_response["cached"] is True
    assert cached_response["results"] == [{"path": "cached"}]
    assert migration_calls == []

    cached_entry[0] = None
    fresh_response = backtest_v7.list_archive_results("mine", session=None)

    assert fresh_response["cached"] is False
    assert fresh_response["results"] == [{"path": "fresh"}]
    assert migration_calls == [("mine", archive_root, "mine", 25)]
    assert cached_writes == [("mine", [{"path": "fresh"}], {"status": "current"})]


def test_archive_overview_uses_manifest_and_safe_scan_counts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Archive overview counts come from a manifest or a safe scan when none exists."""
    archives_root = tmp_path / "archives"
    manifested = archives_root / "manifested"
    scanned = archives_root / "scanned"
    for archive_root in (manifested, scanned):
        git_config = archive_root / ".git/config"
        git_config.parent.mkdir(parents=True)
        git_config.write_text('[remote "origin"]\nurl = https://example.invalid/archive.git\n', encoding="utf-8")

    write_archive_json(
        manifested / ARCHIVE_MANIFEST,
        {
            "schema_version": 1,
            "items": [
                {"type": "backtest_result"},
                {"type": "optimize_config"},
                {"type": "optimize_config"},
            ],
        },
    )
    write_archive_result(scanned / "pbgui/configs/v7.4.2/backtests")
    write_archive_json(
        scanned / "pbgui/configs/v7.4.2/optimize/scanned.json",
        {"config_version": "v7.4.2", "optimize": {}},
    )
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "manifested")
    monkeypatch.setattr(backtest_v7, "_archives_list_cache", {})
    monkeypatch.setattr(backtest_v7, "archive_migration_status", lambda _root, fast=False: {"status": "current"})

    response = backtest_v7.list_archives(session=None)
    by_name = {item["name"]: item for item in response["archives"]}

    assert by_name["manifested"]["results"] == 1
    assert by_name["manifested"]["optimize_configs"] == 2
    assert by_name["manifested"]["manifest"] == {"present": True, "items": 3}
    assert by_name["scanned"]["results"] == 1
    assert by_name["scanned"]["optimize_configs"] == 1
    assert by_name["scanned"]["manifest"] == {"present": False, "items": 0}


def test_optimize_export_migrates_first_and_preserves_valid_sidecar(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated identical exports migrate first and do not rewrite valid metadata."""
    archives_root = tmp_path / "archives"
    archive_root = archives_root / "mine"
    archive_root.mkdir(parents=True)
    local_configs = tmp_path / "optimize"
    source_file = local_configs / "demo.json"
    write_archive_json(source_file, {"config_version": "v7.12.0", "backtest": {}, "optimize": {"n_trials": 10}})
    events = []
    real_resolve = backtest_v7.resolve_optimize_archive_destination

    def fake_load(path: Path, **_kwargs) -> dict:
        """Load isolated PB7 config fixtures as plain JSON objects."""
        return json.loads(Path(path).read_text(encoding="utf-8"))

    def fake_save(config: dict, path: Path) -> None:
        """Save isolated PB7 config fixtures without touching runtime data."""
        write_archive_json(Path(path), config)

    def fake_migrate(*_args, **_kwargs) -> dict:
        """Record migration ordering without performing git work."""
        events.append("migrate")
        return {"status": {"status": "current"}}

    def recording_resolve(*args, **kwargs):
        """Record destination resolution after migration."""
        events.append("resolve")
        return real_resolve(*args, **kwargs)

    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")
    monkeypatch.setattr(backtest_v7, "_opt_archive_configs_dir", lambda: local_configs)
    monkeypatch.setattr(backtest_v7, "load_pb7_config", fake_load)
    monkeypatch.setattr(backtest_v7, "save_pb7_config", fake_save)
    monkeypatch.setattr(backtest_v7, "get_template_config", lambda: {"config_version": "v7.12.0"})
    monkeypatch.setattr(backtest_v7, "maybe_migrate_own_archive", fake_migrate)
    monkeypatch.setattr(backtest_v7, "resolve_optimize_archive_destination", recording_resolve)

    first = backtest_v7._add_optimize_config_to_archive_sync("mine", "demo")
    meta_path = Path(first["path"]).with_name("demo.meta.json")
    first_meta = json.loads(meta_path.read_text(encoding="utf-8"))
    first_mtime = meta_path.stat().st_mtime_ns
    monkeypatch.setattr(
        backtest_v7,
        "write_optimize_meta",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("valid sidecar must not be rewritten")),
    )

    second = backtest_v7._add_optimize_config_to_archive_sync("mine", "demo")

    assert events == ["migrate", "resolve", "migrate", "resolve"]
    assert second["skipped"] is True
    assert second["metadata_repaired"] is False
    assert second["meta"]["created_at"] == first_meta["created_at"]
    assert meta_path.stat().st_mtime_ns == first_mtime


def test_optimize_export_rejects_predictable_tmp_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Optimize archive export never follows the former predictable JSON temp path."""
    archives_root = tmp_path / "archives"
    archive_root = archives_root / "mine"
    archive_root.mkdir(parents=True)
    local_configs = tmp_path / "optimize"
    write_archive_json(
        local_configs / "demo.json",
        {"config_version": "v7.12.0", "backtest": {}, "optimize": {}, "_pbgui_param_status": {"x": 1}},
    )
    destination = archive_root / "pbgui/configs/v7.12.0/optimize/demo.json"
    outside = tmp_path / "outside.json"
    outside.write_text("outside", encoding="utf-8")
    destination.parent.mkdir(parents=True)
    destination.with_suffix(".json.tmp").symlink_to(outside)
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")
    monkeypatch.setattr(backtest_v7, "_opt_archive_configs_dir", lambda: local_configs)
    monkeypatch.setattr(
        backtest_v7,
        "load_pb7_config",
        lambda path, **_kwargs: json.loads(Path(path).read_text(encoding="utf-8")),
    )
    monkeypatch.setattr(
        backtest_v7,
        "maybe_migrate_own_archive",
        lambda *_args, **_kwargs: {"status": {"status": "current"}},
    )

    with pytest.raises(RuntimeError, match="temporary path"):
        backtest_v7._add_optimize_config_to_archive_sync("mine", "demo")

    assert outside.read_text(encoding="utf-8") == "outside"
    assert not destination.exists()


def test_optimize_export_strips_pbgui_param_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Optimize archive export preserves save_pb7_config's UI-status stripping behavior."""
    archives_root = tmp_path / "archives"
    archive_root = archives_root / "mine"
    archive_root.mkdir(parents=True)
    local_configs = tmp_path / "optimize"
    write_archive_json(
        local_configs / "demo.json",
        {
            "config_version": "v7.12.0",
            "backtest": {},
            "optimize": {},
            "_pbgui_param_status": {"long": {"entry_grid_spacing_pct": "added"}},
        },
    )
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")
    monkeypatch.setattr(backtest_v7, "_opt_archive_configs_dir", lambda: local_configs)
    monkeypatch.setattr(
        backtest_v7,
        "load_pb7_config",
        lambda path, **_kwargs: json.loads(Path(path).read_text(encoding="utf-8")),
    )
    monkeypatch.setattr(
        backtest_v7,
        "maybe_migrate_own_archive",
        lambda *_args, **_kwargs: {"status": {"status": "current"}},
    )

    response = backtest_v7._add_optimize_config_to_archive_sync("mine", "demo")
    archived = json.loads(Path(response["path"]).read_text(encoding="utf-8"))

    assert "_pbgui_param_status" not in archived


@pytest.mark.parametrize("sidecar_state", ["missing", "invalid"])
def test_optimize_export_repairs_missing_or_invalid_sidecar(
    sidecar_state: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An identical Optimize export repairs absent or malformed metadata sidecars."""
    archives_root = tmp_path / "archives"
    archive_root = archives_root / "mine"
    archive_root.mkdir(parents=True)
    local_configs = tmp_path / "optimize"
    sidecar_roots = []
    write_archive_json(
        local_configs / "demo.json",
        {"config_version": "v7.12.0", "backtest": {}, "optimize": {"n_trials": 10}},
    )

    def fake_load(path: Path, **_kwargs) -> dict:
        """Load isolated PB7 config fixtures as plain JSON objects."""
        return json.loads(Path(path).read_text(encoding="utf-8"))

    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")
    monkeypatch.setattr(backtest_v7, "_opt_archive_configs_dir", lambda: local_configs)
    monkeypatch.setattr(backtest_v7, "load_pb7_config", fake_load)
    monkeypatch.setattr(backtest_v7, "save_pb7_config", lambda config, path: write_archive_json(Path(path), config))
    monkeypatch.setattr(backtest_v7, "get_template_config", lambda: {"config_version": "v7.12.0"})
    monkeypatch.setattr(
        backtest_v7,
        "write_optimize_meta",
        lambda path, meta, archive_root=None: (
            sidecar_roots.append(archive_root),
            archive_helpers.write_optimize_meta(path, meta, archive_root),
        )[-1],
    )
    monkeypatch.setattr(
        backtest_v7,
        "maybe_migrate_own_archive",
        lambda *_args, **_kwargs: {"status": {"status": "current"}},
    )

    first = backtest_v7._add_optimize_config_to_archive_sync("mine", "demo")
    meta_path = Path(first["path"]).with_name("demo.meta.json")
    if sidecar_state == "missing":
        meta_path.unlink()
    else:
        meta_path.write_text("not-json", encoding="utf-8")

    repaired = backtest_v7._add_optimize_config_to_archive_sync("mine", "demo")
    metadata = json.loads(meta_path.read_text(encoding="utf-8"))

    assert repaired["skipped"] is True
    assert repaired["metadata_repaired"] is True
    assert metadata["name"] == "demo"
    assert metadata["created_at"]
    assert sidecar_roots == [archive_root, archive_root]


def test_optimize_import_collision_modes_and_foreign_source_immutability(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Optimize imports report collisions, overwrite or copy locally, and never alter the source."""
    archives_root = tmp_path / "archives"
    foreign_root = archives_root / "other"
    source_file = foreign_root / "pbgui/configs/v7.12.0/optimize/demo.json"
    source_config = {
        "config_version": "v7.12.0",
        "backtest": {"base_dir": "backtests/pbgui/foreign"},
        "optimize": {"n_trials": 20},
    }
    write_archive_json(source_file, source_config)
    source_bytes = source_file.read_bytes()
    local_configs = tmp_path / "local-optimize"
    write_archive_json(local_configs / "demo.json", {"config_version": "old"})
    write_archive_json(local_configs / "demo_copy.json", {"config_version": "occupied"})

    def fake_load(path: Path, **_kwargs) -> dict:
        """Load isolated PB7 config fixtures as plain JSON objects."""
        return json.loads(Path(path).read_text(encoding="utf-8"))

    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_opt_archive_configs_dir", lambda: local_configs)
    monkeypatch.setattr(backtest_v7, "load_pb7_config", fake_load)
    monkeypatch.setattr(backtest_v7, "save_pb7_config", lambda config, path: write_archive_json(Path(path), config))
    monkeypatch.setattr(backtest_v7, "get_template_config", lambda: {"config_version": "v7.99.0"})

    with pytest.raises(HTTPException) as exc_info:
        backtest_v7.import_archive_optimize_config(
            "other", {"path": str(source_file), "name": "demo", "collision": "error"}, session=None
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == {
        "code": "optimize_config_exists",
        "message": "Optimize config 'demo' already exists",
        "name": "demo",
        "suggested_copy_name": "demo_copy_2",
    }

    overwritten = backtest_v7.import_archive_optimize_config(
        "other", {"path": str(source_file), "name": "demo", "collision": "overwrite"}, session=None
    )
    copied = backtest_v7.import_archive_optimize_config(
        "other", {"path": str(source_file), "name": "demo", "collision": "copy"}, session=None
    )
    overwritten_config = json.loads((local_configs / "demo.json").read_text(encoding="utf-8"))
    copied_config = json.loads((local_configs / "demo_copy_2.json").read_text(encoding="utf-8"))

    assert overwritten == {"ok": True, "name": "demo", "collision": "overwrite"}
    assert copied == {"ok": True, "name": "demo_copy_2", "collision": "copy"}
    assert overwritten_config["backtest"]["base_dir"] == "backtests/pbgui/demo"
    assert copied_config["backtest"]["base_dir"] == "backtests/pbgui/demo_copy_2"
    assert overwritten_config["config_version"] == "v7.12.0"
    assert copied_config["config_version"] == "v7.12.0"
    assert source_file.read_bytes() == source_bytes


def test_concurrent_optimize_copy_imports_choose_distinct_names(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent copy imports serialize name selection through the local save."""
    archives_root = tmp_path / "archives"
    source_file = archives_root / "other/pbgui/configs/v7.12.0/optimize/demo.json"
    write_archive_json(
        source_file,
        {"config_version": "v7.12.0", "backtest": {}, "optimize": {"n_trials": 20}},
    )
    local_configs = tmp_path / "local-optimize"
    barrier = threading.Barrier(3)
    results = []
    errors = []

    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_opt_archive_configs_dir", lambda: local_configs)
    monkeypatch.setattr(
        backtest_v7,
        "load_pb7_config",
        lambda path, **_kwargs: json.loads(Path(path).read_text(encoding="utf-8")),
    )
    monkeypatch.setattr(backtest_v7, "save_pb7_config", lambda config, path: write_archive_json(Path(path), config))
    monkeypatch.setattr(backtest_v7, "get_template_config", lambda: {"config_version": "v7.12.0"})

    def run_import() -> None:
        try:
            barrier.wait(timeout=5)
            results.append(
                backtest_v7.import_archive_optimize_config(
                    "other",
                    {"path": str(source_file), "name": "demo", "collision": "copy"},
                    session=None,
                )
            )
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=run_import) for _ in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait(timeout=5)
    for thread in threads:
        thread.join(timeout=5)

    assert errors == []
    assert all(not thread.is_alive() for thread in threads)
    assert {result["name"] for result in results} == {"demo", "demo_copy"}
    assert {path.stem for path in local_configs.glob("*.json")} == {"demo", "demo_copy"}


@pytest.mark.parametrize(
    ("source_version", "expected_version"),
    [("v7.12.0", "v7.12.0"), (None, "v7.99.0")],
)
def test_optimize_import_preserves_or_injects_config_version(
    source_version: str | None,
    expected_version: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Optimize imports preserve source versions or inject the current template version."""
    archives_root = tmp_path / "archives"
    source_file = archives_root / "other/pbgui/configs/v7.12.0/optimize/source.json"
    source_config = {"backtest": {"base_dir": "foreign/path"}, "optimize": {}}
    if source_version is not None:
        source_config["config_version"] = source_version
    write_archive_json(source_file, source_config)
    source_bytes = source_file.read_bytes()
    local_configs = tmp_path / "local-optimize"

    def fake_load(path: Path, **_kwargs) -> dict:
        """Load isolated PB7 config fixtures as plain JSON objects."""
        return json.loads(Path(path).read_text(encoding="utf-8"))

    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_opt_archive_configs_dir", lambda: local_configs)
    monkeypatch.setattr(backtest_v7, "load_pb7_config", fake_load)
    monkeypatch.setattr(backtest_v7, "save_pb7_config", lambda config, path: write_archive_json(Path(path), config))
    monkeypatch.setattr(backtest_v7, "get_template_config", lambda: {"config_version": "v7.99.0"})

    response = backtest_v7.import_archive_optimize_config(
        "other", {"path": str(source_file), "name": "imported", "collision": "error"}, session=None
    )
    imported = json.loads((local_configs / "imported.json").read_text(encoding="utf-8"))

    assert response == {"ok": True, "name": "imported", "collision": "created"}
    assert imported["config_version"] == expected_version
    assert imported["backtest"]["base_dir"] == "backtests/pbgui/imported"
    assert source_file.read_bytes() == source_bytes


@pytest.mark.parametrize(("removed", "expects_manifest"), [(0, False), (1, True)])
def test_liquidated_route_rebuilds_manifest_only_after_removal(
    removed: int,
    expects_manifest: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Liquidated cleanup rebuilds generated state only when a result was removed."""
    archives_root = tmp_path / "archives"
    archive_root = archives_root / "mine"
    archive_root.mkdir(parents=True)
    rebuilt = []
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")
    monkeypatch.setattr(
        backtest_v7,
        "remove_liquidated_results",
        lambda *_args, **_kwargs: {"ok": True, "matched": removed, "removed": removed},
    )
    monkeypatch.setattr(
        backtest_v7,
        "rebuild_archive_manifest",
        lambda root: rebuilt.append(root) or {"schema_version": 1, "items": []},
    )

    response = backtest_v7.remove_archive_liquidated_results(
        "mine", {"paths": [], "dry_run": False}, session=None
    )

    assert bool(rebuilt) is expects_manifest
    assert ("manifest" in response) is expects_manifest
    if expects_manifest:
        assert rebuilt == [archive_root]


@pytest.mark.parametrize(("removed", "expects_manifest"), [(0, False), (1, True)])
def test_duplicate_route_rebuilds_manifest_only_after_removal(
    removed: int,
    expects_manifest: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Duplicate cleanup leaves cache and manifest untouched for a no-op."""
    archives_root = tmp_path / "archives"
    archive_root = archives_root / "mine"
    archive_root.mkdir(parents=True)
    rebuilt = []
    invalidated = []
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")
    monkeypatch.setattr(
        backtest_v7,
        "remove_duplicate_results",
        lambda *_args, **_kwargs: {"ok": True, "matched": removed, "removed": removed},
    )
    monkeypatch.setattr(backtest_v7, "_invalidate_archive_cache", lambda name=None: invalidated.append(name))
    monkeypatch.setattr(
        backtest_v7,
        "rebuild_archive_manifest",
        lambda root: rebuilt.append(root) or {"schema_version": 1, "items": []},
    )

    response = backtest_v7.remove_archive_duplicate_results(
        "mine", {"paths": [], "dry_run": False}, session=None
    )

    assert bool(rebuilt) is expects_manifest
    assert bool(invalidated) is expects_manifest
    assert ("manifest" in response) is expects_manifest


@pytest.mark.parametrize("archive_state", ["missing", "symlink"])
def test_archive_settings_validate_readme_archive_before_saving_ini(
    archive_state: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An invalid README archive cannot partially persist archive settings."""
    archives_root = tmp_path / "archives"
    archives_root.mkdir()
    if archive_state == "symlink":
        target = tmp_path / "outside"
        target.mkdir()
        (archives_root / "requested").symlink_to(target, target_is_directory=True)
    saved = []
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "save_ini_section", lambda *args: saved.append(args))

    with pytest.raises(HTTPException) as exc_info:
        backtest_v7.save_archive_settings(
            {"my_archive": "requested", "username": "new-user", "readme_title": "Archive"},
            session=None,
        )

    assert exc_info.value.status_code == 404
    assert saved == []


def test_archive_settings_with_readme_can_select_a_new_own_archive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """README-bearing settings validate the requested new selection rather than the old own archive."""
    archives_root = tmp_path / "archives"
    requested = archives_root / "requested"
    requested.mkdir(parents=True)
    saved = []
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "old")
    monkeypatch.setattr(backtest_v7, "save_ini_section", lambda section, values: saved.append((section, values)))
    monkeypatch.setattr(
        backtest_v7,
        "apply_metadata",
        lambda section: {"section": section},
    )

    response = backtest_v7.save_archive_settings(
        {"my_archive": "requested", "username": "new-user", "readme_title": "Requested"},
        session=None,
    )

    assert response == {"ok": True, "apply": {"section": "config_archive"}}
    assert saved == [
        ("config_archive", {"my_archive": "requested", "my_archive_username": "new-user"})
    ]
    assert (requested / "README.md").exists()


def test_archive_settings_without_readme_preserve_unlocked_ini_save(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Settings without README fields retain the existing INI-only behavior."""
    saved = []
    monkeypatch.setattr(
        backtest_v7,
        "archive_transaction",
        lambda _root: (_ for _ in ()).throw(AssertionError("INI-only save must not lock an archive")),
    )
    monkeypatch.setattr(backtest_v7, "save_ini_section", lambda section, values: saved.append((section, values)))
    monkeypatch.setattr(backtest_v7, "apply_metadata", lambda section: {"section": section})

    response = backtest_v7.save_archive_settings(
        {"my_archive": "requested", "username": "new-user", "auto_pull_interval": "15"},
        session=None,
    )

    assert response == {"ok": True, "apply": {"section": "config_archive"}}
    assert saved == [
        (
            "config_archive",
            {
                "my_archive": "requested",
                "my_archive_username": "new-user",
                "auto_pull_interval": "15",
            },
        )
    ]


def test_archive_result_delete_rejects_symlink_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Archive result deletion rejects a symlink even when its target is inside the archive."""
    archives_root = tmp_path / "archives"
    archive_root = archives_root / "mine"
    target = write_archive_result(archive_root / "pbgui/configs/v7.4.2/backtests")
    linked = target.with_name("linked-result")
    linked.symlink_to(target, target_is_directory=True)
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")

    with pytest.raises(HTTPException) as exc_info:
        backtest_v7.delete_archive_result("mine", str(linked), session=None)

    assert exc_info.value.status_code == 400
    assert linked.is_symlink()
    assert target.exists()


@pytest.mark.parametrize(
    ("filename", "route"),
    [("config.json", backtest_v7.get_result_config), ("analysis.json", backtest_v7.get_result_analysis)],
)
def test_archive_config_and_analysis_routes_reject_symlinks(
    filename: str,
    route,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Archive config and analysis reads do not follow symlinked JSON files."""
    archives_root = tmp_path / "archives"
    result_dir = write_archive_result(archives_root / "mine/pbgui/configs/v7.4.2/backtests")
    outside = tmp_path / f"outside-{filename}"
    outside.write_text("{}", encoding="utf-8")
    selected = result_dir / filename
    selected.unlink()
    selected.symlink_to(outside)
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_bt_results_base", lambda: str(tmp_path / "local-results"))
    monkeypatch.setattr(backtest_v7, "_legacy_results_roots", lambda: [])

    with pytest.raises(HTTPException) as exc_info:
        route(str(result_dir), session=None)

    assert exc_info.value.status_code == 404
    assert selected.is_symlink()


@pytest.mark.parametrize("operation", ["get", "import", "delete"])
def test_archive_optimize_routes_reject_symlink_paths(
    operation: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Archive Optimize read, import, and delete routes reject symlinked config paths."""
    archives_root = tmp_path / "archives"
    archive_root = archives_root / "mine"
    optimize_dir = archive_root / "pbgui/configs/v7.12.0/optimize"
    outside = tmp_path / "outside-optimize.json"
    write_archive_json(outside, {"config_version": "v7.12.0", "optimize": {}})
    linked = optimize_dir / "linked.json"
    linked.parent.mkdir(parents=True)
    linked.symlink_to(outside)
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")
    monkeypatch.setattr(backtest_v7, "_opt_archive_configs_dir", lambda: tmp_path / "local-optimize")

    with pytest.raises(HTTPException) as exc_info:
        if operation == "get":
            backtest_v7.get_archive_optimize_config("mine", str(linked), session=None)
        elif operation == "import":
            backtest_v7.import_archive_optimize_config(
                "mine", {"path": str(linked), "name": "linked", "collision": "error"}, session=None
            )
        else:
            backtest_v7.delete_archive_optimize_config("mine", str(linked), session=None)

    assert exc_info.value.status_code == 400
    assert linked.is_symlink()
    assert outside.exists()
