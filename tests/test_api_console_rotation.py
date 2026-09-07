"""Tests for API console rotation before descriptor opening."""

from pathlib import Path
import subprocess
import sys
import threading
import time

import PBApiServer
import logging_helpers


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


def test_api_console_writer_supports_subprocess_redirection(tmp_path, monkeypatch):
    """The lock proxy retains context-manager and descriptor behavior."""
    log_path = tmp_path / "data" / "logs" / "PBApiServer.console.log"
    monkeypatch.setattr(PBApiServer, "PBGDIR", tmp_path)
    monkeypatch.setattr(PBApiServer, "get_rotate_settings", lambda **kwargs: (1024, 1))

    with PBApiServer._open_api_console_log() as writer:
        assert writer.fileno() >= 0
        writer.write("parent\n")
        writer.flush()
        subprocess.run(
            [sys.executable, "-c", "print('child')"],
            stdout=writer,
            stderr=subprocess.STDOUT,
            check=True,
        )

    assert writer.closed is True
    assert log_path.read_text(encoding="utf-8") == "parent\nchild\n"


def test_api_console_write_waits_for_same_inode_purge(tmp_path, monkeypatch):
    """An API-owned console write during purge remains in the active logfile."""
    log_path = tmp_path / "data" / "logs" / "PBApiServer.console.log"
    log_path.parent.mkdir(parents=True)
    monkeypatch.setattr(PBApiServer, "PBGDIR", tmp_path)
    monkeypatch.setattr(PBApiServer, "get_rotate_settings", lambda **kwargs: (1024, 1))

    writer = PBApiServer._open_api_console_log()
    writer.write("before\n")
    snapshot_ready = threading.Event()
    continue_purge = threading.Event()
    write_started = threading.Event()
    write_finished = threading.Event()
    original_replace = logging_helpers._atomic_replace_log_content

    def pause_after_snapshot(path, content, mode):
        snapshot_ready.set()
        assert continue_purge.wait(timeout=2)
        original_replace(path, content, mode)

    monkeypatch.setattr(logging_helpers, "_atomic_replace_log_content", pause_after_snapshot)
    purge_result = []
    purge_thread = threading.Thread(
        target=lambda: purge_result.append(
            logging_helpers.purge_log_to_rotated(str(log_path), 1024, 1)
        )
    )
    purge_thread.start()
    assert snapshot_ready.wait(timeout=2)

    def write_during_purge() -> None:
        write_started.set()
        writer.write("during\n")
        writer.flush()
        write_finished.set()

    write_thread = threading.Thread(target=write_during_purge)
    write_thread.start()
    assert write_started.wait(timeout=2)
    time.sleep(0.05)
    assert not write_finished.is_set()

    continue_purge.set()
    purge_thread.join(timeout=2)
    write_thread.join(timeout=2)
    writer.close()

    assert not purge_thread.is_alive()
    assert not write_thread.is_alive()
    assert purge_result == [(True, "Rotated PBApiServer.console.log with 1 backup generation(s) and truncated the current log")]
    assert Path(f"{log_path}.1").read_text(encoding="utf-8") == "before\n"
    assert log_path.read_text(encoding="utf-8") == "during\n"
