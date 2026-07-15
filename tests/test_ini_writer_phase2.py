"""Focused transactional coverage for Phase 2 local INI writers."""

from __future__ import annotations

import configparser
import os
from pathlib import Path
import threading

import pbgui_purefunc


def _isolate_ini(monkeypatch, path: Path) -> None:
    """Redirect canonical PBGui INI access to one temporary file."""
    monkeypatch.setattr(pbgui_purefunc, "pbgui_ini_path", lambda: path)


def test_pbdata_api_save_publishes_one_generation(tmp_path, monkeypatch) -> None:
    """One PBData API save publishes users and settings in one generation."""
    from api import services

    path = tmp_path / "pbgui.ini"
    path.write_text("[unrelated]\nvalue = keep\n", encoding="utf-8")
    _isolate_ini(monkeypatch, path)
    replacements = []
    real_replace = os.replace

    def record_replace(source, destination):
        replacements.append(Path(destination))
        real_replace(source, destination)

    monkeypatch.setattr(os, "replace", record_replace)
    body = services.PBDataSettings(
        fetch_users=["alice"],
        trades_users=["bob"],
        shared_rest_user_pause_seconds=1.5,
        shared_rest_pause_by_exchange={"bybit": 2.0, "hyperliquid": 1.5},
    )

    result = services.save_pbdata_settings(body, session=None)
    assert result["ok"] is True
    assert result["apply"]["message"] == "Applies next cycle"
    assert len([item for item in replacements if item == path]) == 1
    snapshot = pbgui_purefunc.load_ini_snapshot(path)
    assert snapshot.get("pbdata", "fetch_users") == "['alice']"
    assert snapshot.get("pbdata", "trades_users") == "['bob']"
    assert snapshot.get("pbdata", "shared_rest_pause_by_exchange_json") == '{"bybit": 2.0}'
    assert snapshot.get("unrelated", "value") == "keep"


def test_tradfi_and_logging_preserve_concurrent_keys(tmp_path, monkeypatch) -> None:
    """Logging writes retain keys published by concurrent INI transactions."""
    import logging_helpers

    path = tmp_path / "pbgui.ini"
    path.write_text("[main]\nkeep = yes\n", encoding="utf-8")
    _isolate_ini(monkeypatch, path)
    monkeypatch.setattr(logging_helpers, "PBGUI_INI", path)
    barrier = threading.Barrier(3)

    def logging_writer() -> None:
        barrier.wait()
        logging_helpers.set_rotate_defaults(2048, 7)

    threads = [threading.Thread(target=logging_writer), threading.Thread(target=logging_writer)]
    for thread in threads:
        thread.start()
    barrier.wait()
    pbgui_purefunc.save_ini("concurrent", "value", "kept")
    for thread in threads:
        thread.join()

    snapshot = pbgui_purefunc.load_ini_snapshot(path)
    assert not snapshot.parser.has_section("tradfi_profiles")
    assert snapshot.get("logging", "rotate_default_backup_count") == "7"
    assert snapshot.get("main", "keep") == "yes"
    assert snapshot.get("concurrent", "value") == "kept"


def test_vps_monitor_alert_save_publishes_one_generation(monkeypatch) -> None:
    """The reusable alert writer batches token and route updates."""
    import master.async_monitor as async_monitor

    generations = []

    def capture_update(mutator) -> None:
        parser = configparser.ConfigParser()
        mutator(parser)
        generations.append(parser)

    owner = async_monitor.VPSMonitor.__new__(async_monitor.VPSMonitor)
    owner._telegram_token = "old"
    owner._telegram_chat_id = "old-chat"
    reloads = []
    owner._load_alert_routes = lambda *, force=False: reloads.append(force)
    monkeypatch.setattr(async_monitor, "update_ini", capture_update)

    owner.save_alert_settings({
        "telegram_token": " new-token ",
        "telegram_chat_id": " new-chat ",
        "offline_gui": False,
        "ssh_lost_telegram": True,
    })

    assert len(generations) == 1
    parser = generations[0]
    assert parser.get("main", "telegram_token") == "new-token"
    assert parser.get("main", "telegram_chat_id") == "new-chat"
    assert parser.get("vps_monitor_alerts", "offline_gui") == "false"
    assert parser.get("vps_monitor_alerts", "ssh_lost_telegram") == "true"
    assert owner._telegram_token == "new-token"
    assert owner._telegram_chat_id == "new-chat"
    assert reloads == [True]
