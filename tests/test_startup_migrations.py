"""Tests for process-safe startup migrations."""

import json
import os
from pathlib import Path

import pytest

import startup_migrations


def _run(root: Path, monkeypatch: pytest.MonkeyPatch) -> dict:
    """Run migrations with the global test skip explicitly disabled."""
    monkeypatch.delenv("PBGUI_SKIP_STARTUP_MIGRATIONS", raising=False)
    return startup_migrations.run_startup_migrations(root)


def test_logging_cleanup_exact_allowlist_and_idempotency(tmp_path, monkeypatch):
    """Delete exact obsolete families while preserving current and similar names."""
    logs = tmp_path / "data" / "logs"
    logs.mkdir(parents=True)
    removed = (
        "ApiLogging.log", "ApiLogging.log.old", "ApiLogging.log.lock",
        "ApiLogging.log.12", "income_other_x.json", "PBRemote.log",
        "PBRemote.log.3", "PBMon.log", "sync.log.old",
        "FastAPI.log", "FileSync.log.2", "PBStat.log", "V7ConfigSync.log",
        "config_archives.log", "Auth.log", "LiveSession.log.old",
    )
    kept = ("ApiLogging-extra.log", "ApiLogging.log.bak", "PBRemote.log.bak", "current.log")
    for name in removed + kept:
        (logs / name).write_text(name, encoding="utf-8")
    (tmp_path / "api_server.log").write_text("root", encoding="utf-8")
    (tmp_path / "pbgui.ini").write_text(
        "# Preserve this comment and formatting.\n[main]\npbname = keep\n[pbremote]\nbucket = old\n"
        "[dashboard]\nrefresh_interval_sec = 5\n"
        "[v7_grid_visualizer]\nmovie_preset = old\n"
        "[v7_strategy_explorer]\nmovie_preset = old\n"
        "[streamlit]\nport = 8501\n"
        "[optimize]\ncpu = 2\n[backtest]\nautostart = true\n"
        "[backtest_multi]\nautostart = true\n[optimize_multi]\nautostart = false\n",
        encoding="utf-8",
    )

    result = _run(tmp_path, monkeypatch)
    state = json.loads((tmp_path / "data" / "state" / "startup_migrations.json").read_text())

    assert result["completed"] == [
        startup_migrations.MIGRATION_ID,
        startup_migrations.RETIRED_SERVICE_LOGS_MIGRATION_ID,
        startup_migrations.LOG_INVENTORY_CLEANUP_MIGRATION_ID,
        startup_migrations.OBSOLETE_INI_SECTIONS_MIGRATION_ID,
    ]
    assert all(not (logs / name).exists() for name in removed)
    assert all((logs / name).exists() for name in kept)
    record = state["completed"][startup_migrations.MIGRATION_ID]
    assert set(record) == {"names", "count", "bytes", "timestamp"}
    assert startup_migrations.run_startup_migrations(tmp_path)["completed"] == []
    assert oct(os.stat(tmp_path / "data" / "state" / "startup_migrations.json").st_mode & 0o777) == "0o600"
    migrated_ini = (tmp_path / "pbgui.ini").read_text(encoding="utf-8")
    assert all(
        f"[{section}]" not in migrated_ini
        for section in ("pbremote", "dashboard", "v7_grid_visualizer", "v7_strategy_explorer", "streamlit")
    )
    assert "[main]" in migrated_ini
    assert "# Preserve this comment and formatting.\n[main]\npbname = keep\n" in migrated_ini
    assert all(
        f"[{section}]" in migrated_ini
        for section in ("optimize", "backtest", "backtest_multi", "optimize_multi")
    )
    assert (tmp_path / "pbgui.ini").stat().st_mode & 0o777 == 0o600


def test_failure_is_not_completed_and_retries(tmp_path, monkeypatch):
    """A failed deletion must remain pending for the next startup."""
    logs = tmp_path / "data" / "logs"
    logs.mkdir(parents=True)
    target = logs / "ApiKeys.log"
    target.write_text("x", encoding="utf-8")
    real_unlink = Path.unlink

    def fail_once(path, *args, **kwargs):
        if path == target.resolve():
            raise OSError("busy")
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_once)
    with pytest.raises(OSError):
        _run(tmp_path, monkeypatch)
    assert not (tmp_path / "data" / "state" / "startup_migrations.json").exists()
    monkeypatch.setattr(Path, "unlink", real_unlink)
    assert _run(tmp_path, monkeypatch)["completed"] == [
        startup_migrations.MIGRATION_ID,
        startup_migrations.RETIRED_SERVICE_LOGS_MIGRATION_ID,
        startup_migrations.LOG_INVENTORY_CLEANUP_MIGRATION_ID,
        startup_migrations.OBSOLETE_INI_SECTIONS_MIGRATION_ID,
    ]


def test_symlink_is_rejected_without_touching_target(tmp_path, monkeypatch):
    """Never follow an allowlisted symlink during cleanup."""
    logs = tmp_path / "data" / "logs"
    logs.mkdir(parents=True)
    outside = tmp_path / "outside.txt"
    outside.write_text("keep", encoding="utf-8")
    (logs / "ApiLogging.log").symlink_to(outside)
    with pytest.raises(RuntimeError, match="symlink"):
        _run(tmp_path, monkeypatch)
    assert outside.read_text(encoding="utf-8") == "keep"


def test_skip_environment_does_not_create_state(tmp_path, monkeypatch):
    """The skip switch must avoid all migration filesystem access."""
    monkeypatch.setenv("PBGUI_SKIP_STARTUP_MIGRATIONS", "1")
    assert startup_migrations.run_startup_migrations(tmp_path) == {"skipped": True, "completed": []}
    assert not (tmp_path / "data").exists()
