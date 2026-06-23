"""Contract tests for PB7 metadata, archive layout, and PB7 runtime preflights."""

from __future__ import annotations

import json
import subprocess
import datetime
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
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "")
    monkeypatch.setattr(backtest_v7, "_resolve_result_dir", lambda path: Path(path).resolve())

    response = backtest_v7.add_config_to_archive("mine", {"source_path": str(result_dir)}, session=None)

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
    assert '<th width="48%">Config</th>' in scores_page
    assert 'title="' in scores_page
    assert '<th width="14%">Result</th>' not in scores_page
    assert "PBGui Score Table" in scores_html
    assert "score-data" in scores_html
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

    def fake_run(cmd, cwd=None, capture_output=True, text=True, timeout=None, input=None):
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


def test_compact_archive_uses_force_with_lease_sequence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Archive compaction creates a root commit and force-pushes it with an explicit lease."""
    archives_root = tmp_path / "archives"
    archive_root = archives_root / "mine"
    archive_root.mkdir(parents=True)
    commands = []

    def fake_run(cmd, cwd=None, capture_output=True, text=True, timeout=None):
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
