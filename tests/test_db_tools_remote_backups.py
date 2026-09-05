"""Run the actual remote backup payload locally, never importing API startup."""

import ast
import asyncio
import shlex
import signal
import sqlite3
import subprocess
import sys
import time
import uuid
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def tools():
    """Extract only the production backup helpers, without runtime configuration."""
    names = {
        "_remote_sqlite_backup_command", "_backup_remote_file", "_validate_backup_name",
        "_backup_db_name", "_backup_label",
    }
    source = Path(__file__).resolve().parents[1] / "api" / "db_tools.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    body = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
    assert {node.name for node in body} == names
    namespace = {
        "Path": Path, "shlex": shlex, "uuid": uuid,
        "DB_FILE_NAMES": ("pbgui.db", "pbgui_trades.db"),
        "_timestamp": lambda: "20260905-120000",
        "_remote_path": lambda target, *parts: "/isolated/" + "/".join(parts),
    }
    exec(compile(ast.Module(body=ast.parse("from __future__ import annotations").body + body,
                            type_ignores=[]), str(source), "exec"), namespace)
    return SimpleNamespace(**namespace, namespace=namespace)


def _database(path):
    """Create a committed temporary database and return its owned connection."""
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE sample (value TEXT)")
    conn.execute("INSERT INTO sample VALUES ('original')")
    conn.commit()
    return conn


def _argv(tools, source, destination, timeout=30):
    """Replace only the remote interpreter with the current test venv interpreter."""
    argv = shlex.split(tools._remote_sqlite_backup_command(str(source), str(destination), timeout=timeout))
    assert argv[0:2] == ["python3", "-c"]
    return [sys.executable, "-B", *argv[1:]]


@pytest.mark.parametrize("existing", ["file", "symlink", "dangling"])
def test_remote_backup_preserves_existing_destination(tmp_path, tools, existing):
    """A collision never overwrites a file or follows/removes a destination symlink."""
    source, destination, sentinel = tmp_path / "source.db", tmp_path / "backup.db", tmp_path / "sentinel"
    with closing(_database(source)):
        pass
    sentinel.write_bytes(b"keep sentinel")
    if existing == "file":
        destination.write_bytes(b"previous snapshot")
    else:
        destination.symlink_to(sentinel if existing == "symlink" else tmp_path / "missing")
    before = destination.lstat()
    result = subprocess.run(_argv(tools, source, destination), cwd=tmp_path, capture_output=True, timeout=5)
    assert result.returncode != 0 and b"FileExistsError" in result.stderr
    assert destination.lstat().st_ino == before.st_ino
    assert sentinel.read_bytes() == b"keep sentinel"
    if existing == "file":
        assert destination.read_bytes() == b"previous snapshot"
    else:
        assert destination.is_symlink()
    assert not list(tmp_path.glob(".sqlite-backup-*"))


@pytest.mark.parametrize("failure", ["missing", "invalid", "busy"])
def test_remote_backup_failure_keeps_previous_snapshot(tmp_path, tools, failure):
    """Source/open/copy failures clean up only the helper's private staging files."""
    source, destination = tmp_path / "source.db", tmp_path / "backup.db"
    destination.write_bytes(b"previous snapshot")
    with closing(_database(source)) as writer:
        if failure == "missing":
            source = tmp_path / "absent.db"
        elif failure == "invalid":
            source = tmp_path / "invalid.db"
            source.write_bytes(b"not sqlite")
        else:
            writer.execute("BEGIN EXCLUSIVE")
        result = subprocess.run(_argv(tools, source, destination, timeout=0.05), cwd=tmp_path, capture_output=True, timeout=5)
        writer.rollback()
    assert result.returncode != 0
    if failure == "busy":
        assert b"deadline exceeded" in result.stderr
    assert destination.read_bytes() == b"previous snapshot"
    assert not list(tmp_path.glob(".sqlite-backup-*"))


def test_remote_backup_contains_wal_and_escapes_paths(tmp_path, tools):
    """The argv payload captures committed WAL data and quotes URI/shell metacharacters."""
    source, destination = tmp_path / "source ?#' db", tmp_path / "backups" / "target ' db"
    with closing(_database(source)) as writer:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("UPDATE sample SET value='wal'")
        writer.commit()
        assert Path(str(source) + "-wal").exists()
        result = subprocess.run(_argv(tools, source, destination), cwd=tmp_path, capture_output=True, timeout=5)
        assert result.returncode == 0, result.stderr
    with closing(sqlite3.connect(destination)) as snapshot:
        assert snapshot.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert snapshot.execute("SELECT value FROM sample").fetchone() == ("wal",)
    assert destination.stat().st_mode & 0o777 == 0o600
    assert destination.parent.stat().st_mode & 0o777 == 0o700
    assert not list(destination.parent.glob(".sqlite-backup-*"))


def test_remote_backup_concurrent_publication_has_one_winner(tmp_path, tools):
    """Two local helper processes targeting the same snapshot cannot replace the winner."""
    sources = [tmp_path / f"source-{index}.db" for index in range(2)]
    destination = tmp_path / "backup.db"
    for index, source in enumerate(sources):
        with closing(_database(source)) as conn:
            conn.execute("UPDATE sample SET value=?", (str(index),))
            conn.commit()
    processes = [subprocess.Popen(_argv(tools, source, destination), cwd=tmp_path,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE) for source in sources]
    try:
        outputs = [process.communicate(timeout=5) for process in processes]
        assert sorted(process.returncode for process in processes) == [0, 1]
        winner = next(index for index, process in enumerate(processes) if process.returncode == 0)
        assert b"FileExistsError" in outputs[1 - winner][1]
        with closing(sqlite3.connect(destination)) as snapshot:
            assert snapshot.execute("SELECT value FROM sample").fetchone() == (str(winner),)
        assert not list(tmp_path.glob(".sqlite-backup-*"))
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=5)


@pytest.mark.parametrize("db_name", ["pbgui.db", "pbgui_trades.db"])
def test_remote_backup_same_second_names_are_distinct(tools, db_name):
    """Same-label backups use UUIDs while retaining the list/restore filename contract."""
    calls = []

    async def run(target, command, *, timeout):
        """Capture commands instead of connecting to any remote host."""
        calls.append((target, command, timeout))
        return SimpleNamespace(returncode=0)

    tools.namespace["_pool"] = lambda: SimpleNamespace(run=run)
    first = asyncio.run(tools._backup_remote_file("mock", f"/isolated/data/{db_name}", "cleanup"))
    second = asyncio.run(tools._backup_remote_file("mock", f"/isolated/data/{db_name}", "cleanup"))
    assert first != second
    for backup in (first, second):
        name = Path(backup).name
        assert tools._validate_backup_name(name) == name
        assert tools._backup_db_name(name) == db_name
        assert tools._backup_label(name, db_name).endswith("-cleanup")
    assert len(calls) == 2 and all(call[2] > 30 for call in calls)


def test_remote_backup_rejects_symlink_publication_directory(tmp_path, tools):
    """A replaced final publication directory cannot redirect output through a symlink."""
    source = tmp_path / "source.db"
    with closing(_database(source)):
        pass
    actual = tmp_path / "actual"
    actual.mkdir()
    redirected = tmp_path / "redirected"
    redirected.symlink_to(actual, target_is_directory=True)
    result = subprocess.run(_argv(tools, source, redirected / "snapshot.db"), cwd=tmp_path, capture_output=True, timeout=5)
    assert result.returncode != 0
    assert list(actual.iterdir()) == []


def test_remote_backup_signal_cleans_private_snapshot_only(tmp_path, tools):
    """Signal a test-owned busy backup process and retain its existing destination."""
    source, destination = tmp_path / "source.db", tmp_path / "snapshot.db"
    destination.write_bytes(b"previous snapshot")
    with closing(_database(source)) as writer:
        writer.execute("BEGIN EXCLUSIVE")
        process = subprocess.Popen(_argv(tools, source, destination), cwd=tmp_path,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            end = time.monotonic() + 5
            while not list(tmp_path.glob(".sqlite-backup-*")) and time.monotonic() < end:
                time.sleep(0.01)
            assert list(tmp_path.glob(".sqlite-backup-*"))
            process.send_signal(signal.SIGTERM)
            _, stderr = process.communicate(timeout=5)
            assert process.returncode != 0 and b"backup cancelled" in stderr
            assert not list(tmp_path.glob(".sqlite-backup-*"))
            assert destination.read_bytes() == b"previous snapshot"
        finally:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=5)
            writer.rollback()
