"""Tests for API console rotation before descriptor opening."""

from pathlib import Path

import PBApiServer


def test_api_console_rotates_before_open(tmp_path, monkeypatch):
    """The console file is rotated before an append descriptor is returned."""
    log_path = tmp_path / "data" / "logs" / "PBApiServer.console.log"
    log_path.parent.mkdir(parents=True)
    log_path.write_text("old console", encoding="utf-8")
    events = []
    monkeypatch.setattr(PBApiServer, "PBGDIR", tmp_path)
    monkeypatch.setattr(PBApiServer, "get_rotate_settings", lambda **kwargs: (1, 2))

    def rotate(path, max_bytes, backup_count):
        events.append((path, max_bytes, backup_count))
        Path(path).replace(f"{path}.1")

    monkeypatch.setattr(PBApiServer, "rotate_logfile_if_oversize", rotate)
    handle = PBApiServer._open_api_console_log()
    try:
        handle.write("new")
    finally:
        handle.close()
    assert events == [(str(log_path), 1, 2)]
    assert (log_path.parent / "PBApiServer.console.log.1").read_text() == "old console"
    assert log_path.read_text() == "new"
