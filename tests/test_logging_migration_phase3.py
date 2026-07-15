"""Focused regression tests for Phase 3 runtime logging migration."""

from pathlib import Path

import pytest

import Exchange as exchange_module
import vps_manager_core


def test_save_income_other_emits_structured_diagnostic_without_raw_file(tmp_path, monkeypatch):
    """Unimported income should be centrally logged instead of appended as raw JSON."""
    events = []
    monkeypatch.setattr(exchange_module, "PBGDIR", tmp_path)
    monkeypatch.setattr(exchange_module, "_log", lambda *args, **kwargs: events.append((args, kwargs)))
    exchange = exchange_module.Exchange.__new__(exchange_module.Exchange)
    history = [{"symbol": "BTCUSDT", "api_key": "secret-value"}]

    assert exchange.save_income_other(history, "demo") is None

    assert not (tmp_path / "data" / "logs" / "income_other_demo.json").exists()
    args, kwargs = events[0]
    assert args[0] == exchange_module.SERVICE
    assert kwargs["meta"] == {
        "operation": "save_income_other",
        "exchange": "demo",
        "record_count": 1,
        "records": history,
    }


@pytest.mark.parametrize("method_name", ["_append_task_log", "_start_task_log"])
def test_vps_task_log_alias_copy_failure_is_logged(tmp_path, monkeypatch, method_name):
    """Alias-copy failures should retain the transcript and emit host/task context."""
    events = []
    run_log = tmp_path / "run.log"
    alias_log = tmp_path / "alias.log"
    vps = vps_manager_core.VPS()
    vps._hostname = "test-host"
    vps.command = "vps-update"
    monkeypatch.setattr(vps, "_task_log_path", lambda *args: run_log)
    monkeypatch.setattr(vps, "_task_log_alias_path", lambda *args: alias_log)
    monkeypatch.setattr(vps_manager_core.shutil, "copyfile", lambda *args: (_ for _ in ()).throw(OSError("copy failed")))
    monkeypatch.setattr(vps_manager_core, "_log", lambda *args, **kwargs: events.append((args, kwargs)))

    if method_name == "_append_task_log":
        getattr(vps, method_name)("transcript\n", task_name="vps-update", fallback="vps-update", buffer_attr="update_log")
        assert run_log.read_text(encoding="utf-8") == "transcript\n"
        assert vps.update_log == "transcript\n"
    else:
        getattr(vps, method_name)(task_name="vps-update", fallback="vps-update")
        assert "PLAYBOOK RUN START" in run_log.read_text(encoding="utf-8")

    args, kwargs = events[0]
    assert args[0] == vps_manager_core.SERVICE
    assert kwargs["level"] == "WARNING"
    assert kwargs["meta"]["host"] == "test-host"
    assert kwargs["meta"]["task"] == "vps-update"
    assert kwargs["meta"]["operation"] == "copy_task_log_alias"
