"""Focused tests for request and high-risk operational logging context."""

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from starlette.requests import Request
from starlette.responses import Response

import Database as database_mod
import PBApiServer
import logging_helpers


def _request(path: str, request_id: str) -> Request:
    """Build an isolated HTTP request with a request ID header."""
    return Request({
        "type": "http",
        "method": "POST",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "headers": [(b"x-request-id", request_id.encode())],
        "scheme": "http",
        "server": ("testserver", 80),
        "client": ("127.0.0.1", 1234),
        "root_path": "",
    })


@pytest.mark.parametrize("supplied, propagated", [("safe.ID-_42", "safe.ID-_42"), ("bad id/secret", None)])
def test_request_id_validation_propagation_and_log_context(tmp_path, monkeypatch, supplied, propagated):
    """Middleware should propagate safe IDs, replace invalid IDs, and bind log context."""
    logfile = tmp_path / "request.log"
    monkeypatch.setattr(PBApiServer, "unauthenticated_page_redirect", lambda *_args: None)

    async def call_next(_request):
        logging_helpers.human_log("RequestTest", "inside", logfile=str(logfile))
        return Response(status_code=204)

    response = asyncio.run(PBApiServer.redirect_unauthenticated_page(_request("/api/test", supplied), call_next))
    request_id = response.headers["X-Request-ID"]
    if propagated is None:
        assert request_id != supplied
        assert PBApiServer._REQUEST_ID_RE.fullmatch(request_id)
    else:
        assert request_id == propagated

    inside = logfile.read_text(encoding="utf-8").splitlines()[0]
    metadata = json.loads(inside[inside.index("{"):])
    assert metadata == {"operation": "POST /api/test", "request_id": request_id}

    logging_helpers.human_log("RequestTest", "outside", logfile=str(logfile))
    assert "request_id" not in logfile.read_text(encoding="utf-8").splitlines()[1]


def test_database_backup_and_restore_logs_operations(tmp_path, monkeypatch):
    """Database backup and restore events should carry stable operation metadata."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    database = database_mod.Database.__new__(database_mod.Database)
    database.db = data_dir / "pbgui.db"
    database.db.write_text("original", encoding="utf-8")
    events = []
    monkeypatch.setattr(database_mod, "PBGDIR", tmp_path)
    monkeypatch.setattr(database_mod, "_human_log", lambda *args, **kwargs: events.append((args, kwargs)))

    backup_path = database.backup_full_db()
    database.db.write_text("changed", encoding="utf-8")
    assert database.restore_db_from(backup_path) is True

    assert [event[1]["meta"]["operation"] for event in events] == ["backup_full_db", "restore_db_from"]
    assert database.db.read_text(encoding="utf-8") == "original"


def test_runv7_start_and_stop_logs_instance_operations(tmp_path, monkeypatch):
    """RunV7 process lifecycle logs should identify the operation and instance."""
    pbrun = pytest.importorskip("PBRun")
    runner = pbrun.RunV7()
    runner.path = str(tmp_path)
    runner.user = "bot-a"
    runner.pb7dir = str(tmp_path)
    runner.pb7venv = "/usr/bin/python3"
    runner.pbgdir = str(tmp_path)
    events = []
    state = {"running": False}
    process = SimpleNamespace(stderr=iter(()))

    monkeypatch.setattr(runner, "is_running", lambda: state["running"])
    monkeypatch.setattr(runner, "_cluster_gate_result", lambda: {"ok": True, "status": "not_configured"})
    monkeypatch.setattr(runner, "_cluster_gate_allows_run", lambda: True)
    monkeypatch.setattr(pbrun.subprocess, "Popen", lambda *_args, **_kwargs: (state.update(running=True) or process))
    monkeypatch.setattr(pbrun.threading, "Thread", lambda *args, **kwargs: SimpleNamespace(start=lambda: None))
    monkeypatch.setattr(pbrun, "_log", lambda *args, **kwargs: events.append((args, kwargs)))
    monkeypatch.setattr(runner, "pid", lambda: process)
    monkeypatch.setattr(pbrun, "_kill_process", lambda *_args: state.update(running=False))

    runner.start(reload_config=False)
    runner.stop()

    lifecycle = [event for event in events if event[1].get("meta", {}).get("operation")]
    assert [event[1]["meta"] for event in lifecycle] == [
        {"operation": "start_passivbot_v7", "instance": "bot-a"},
        {"operation": "stop_passivbot_v7", "instance": "bot-a"},
    ]
    assert all(event[1]["user"] == "bot-a" for event in lifecycle)
